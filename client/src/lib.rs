use std::{os::fd::AsFd, path::Path};

use futures_util::{future::OptionFuture, SinkExt, StreamExt};
use protocol::ProcessState;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::UnixStream,
    select,
};
use withfd::WithFdExt;

/// The actual user interface
///
/// Each instance represents a user
pub struct Client {
    /// Control channel to the server
    server_ctrl: std::os::unix::net::UnixStream,
}

struct TerminalStateGuard {
    original: nix::sys::termios::Termios,
}

impl Drop for TerminalStateGuard {
    fn drop(&mut self) {
        nix::sys::termios::tcsetattr(
            std::io::stdin().as_fd(),
            nix::sys::termios::SetArg::TCSANOW,
            &self.original,
        )
        .unwrap();
    }
}

struct LogFuture<F>(F);
impl<F: std::future::Future> std::future::Future for LogFuture<F>
where
    F::Output: std::fmt::Debug,
{
    type Output = F::Output;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context,
    ) -> std::task::Poll<Self::Output> {
        tracing::info!("Polling {}", std::any::type_name::<F>());
        let poll = unsafe {
            let inner = std::pin::Pin::into_inner_unchecked(self);
            std::pin::Pin::new_unchecked(&mut inner.0).poll(cx)
        };
        tracing::info!("Poll {:?}", poll);
        poll
    }
}

pub trait UserInterface {
    fn print_processes(&mut self, processes: &[protocol::Process]);
}

pub fn signal_to_string(signal: i32) -> nix::Result<&'static str> {
    Ok(nix::sys::signal::Signal::try_from(signal)?.as_str())
}

fn get_term_size(fd: i32) -> std::io::Result<(u16, u16)> {
    use nix::libc::winsize;
    nix::ioctl_read_bad!(_get_window_size, libc::TIOCGWINSZ, winsize);

    let mut size = winsize {
        ws_col:    0,
        ws_row:    0,
        ws_xpixel: 0,
        ws_ypixel: 0,
    };

    let _ = unsafe { _get_window_size(fd, &mut size) }?;

    Ok((size.ws_col, size.ws_row))
}

impl Client {
    pub fn new(start_server: Option<fn() -> std::io::Result<()>>) -> std::io::Result<Self> {
        let uid = nix::unistd::getuid();
        let path = Path::new(protocol::BASE_DIR)
            .join(uid.to_string())
            .join("job-security")
            .join("sock");
        let server_ctrl = std::os::unix::net::UnixStream::connect(&path);
        match server_ctrl {
            Ok(server_ctrl) => return Ok(Self { server_ctrl }),
            Err(e)
                if (e.kind() == std::io::ErrorKind::NotFound ||
                    e.kind() == std::io::ErrorKind::ConnectionRefused) &&
                    start_server.is_some() => {},
            Err(e) => return Err(e),
        }

        // Try to start the server
        let start_server = start_server.unwrap();
        start_server()?;

        // Retry after server is started
        let server_ctrl = std::os::unix::net::UnixStream::connect(&path)?;
        Ok(Self { server_ctrl })
    }

    pub async fn run(
        self,
        ui: &mut impl UserInterface,
        mut req: protocol::Request,
        detach: bool,
    ) -> std::io::Result<Option<ProcessState>> {
        let Self { server_ctrl } = self;
        server_ctrl.set_nonblocking(true)?;
        let server_ctrl: UnixStream = server_ctrl.try_into()?;
        let mut server_ctrl =
            tokio_util::codec::Framed::new(server_ctrl.with_fd(), protocol::client_codec());

        if let protocol::Request::Start { cols, rows, .. } = &mut req {
            if *cols == 0 && *rows == 0 {
                let (term_cols, term_rows) = get_term_size(nix::libc::STDOUT_FILENO)?;
                *cols = term_cols;
                *rows = term_rows;
            }
        }

        server_ctrl.send(req.clone()).await?;
        if detach {
            return Ok(None)
        }

        #[allow(clippy::single_match)]
        match req {
            protocol::Request::ListProcesses => {
                let mut processes = Vec::new();
                while let Some(msg) = server_ctrl.next().await {
                    match msg? {
                        protocol::Event::Process(p) => processes.push(p),
                        _ => unreachable!("Server sent an unexpected event"),
                    }
                }
                ui.print_processes(&processes);
                return Ok(None)
            },
            _ => (),
        }
        // Change terminal settings:
        //  - Disable echo
        //  - Turn on raw mode
        //  - Disable Ctrl-C, Ctrl-Z, Ctrl-S, Ctrl-Q, Ctrl-\ signals

        let _guard = {
            use nix::sys::termios::{InputFlags, LocalFlags, OutputFlags};
            let original_termios = nix::sys::termios::tcgetattr(std::io::stdin().as_fd()).unwrap();
            let mut termios = original_termios.clone();
            termios.local_flags &= !(LocalFlags::ECHO |
                LocalFlags::ECHONL |
                LocalFlags::ICANON |
                LocalFlags::ISIG |
                LocalFlags::IEXTEN);
            termios.output_flags &= !OutputFlags::OPOST;
            termios.input_flags &= !(InputFlags::BRKINT |
                InputFlags::IGNBRK |
                InputFlags::PARMRK |
                InputFlags::ICRNL |
                InputFlags::IGNCR |
                InputFlags::INLCR |
                InputFlags::ISTRIP |
                InputFlags::IXON);
            termios.control_chars = [nix::sys::termios::_POSIX_VDISABLE; libc::NCCS];
            termios.control_chars[nix::sys::termios::SpecialCharacterIndices::VMIN as usize] = 1;

            nix::sys::termios::tcsetattr(
                std::io::stdin().as_fd(),
                nix::sys::termios::SetArg::TCSANOW,
                &termios,
            )
            .unwrap();
            TerminalStateGuard {
                original: original_termios,
            }
        };
        let mut data_buf = [0u8; 1024];
        let mut data_channel: Option<UnixStream> = None;
        let (stdin_tx, mut stdin_rx) = tokio::sync::mpsc::channel(1);
        let (stdout_tx, stdout_rx) = tokio::sync::mpsc::unbounded_channel();
        std::thread::spawn(move || {
            let mut stdin_buf = [0u8; 1024];
            loop {
                let nbytes = nix::unistd::read(libc::STDIN_FILENO, &mut stdin_buf[..]).unwrap();
                stdin_tx
                    .blocking_send(stdin_buf[..nbytes].to_vec())
                    .unwrap();
            }
        });
        let stdout_thread = std::thread::spawn(move || {
            let mut stdout_rx: tokio::sync::mpsc::UnboundedReceiver<Vec<u8>> = stdout_rx;
            while let Some(data) = stdout_rx.blocking_recv() {
                let mut data = &data[..];
                while !data.is_empty() {
                    let nbytes = nix::unistd::write(std::io::stdout().as_fd(), data).unwrap();
                    data = &data[nbytes..];
                }
            }
        });
        let mut total_received = 0;
        let mut signal =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::window_change())?;
        let mut id = None;
        let status = loop {
            select! {
                event = server_ctrl.next() => {
                    tracing::info!("event: {:?}", event);
                    let Some(event) = event else {
                        return Err(std::io::ErrorKind::UnexpectedEof.into())
                    };
                    let event = event?;
                    match event {
                        protocol::Event::StateChanged { state: ProcessState::Running, id: remote_id } => {
                            id = Some(remote_id);
                            let fd = server_ctrl.get_mut().take_fds().next().unwrap();
                            data_channel = Some(std::os::unix::net::UnixStream::from(fd).try_into()?);
                        },
                        protocol::Event::StateChanged { state: state @ ProcessState::Stopped, .. } => {
                            break Some(state);
                        }
                        protocol::Event::StateChanged { state: state @ ProcessState::Terminated(_), .. } => {
                            break Some(state);
                        }
                        protocol::Event::Error(err) => {
                            return Err(std::io::Error::new(
                                std::io::ErrorKind::Other,
                                format!("server rejected request {req:?}: {err}")
                            ));
                        }
                        protocol::Event::WindowSize { cols, rows } => {
                            if let Some(id) = id {
                                let (our_cols, our_rows) = get_term_size(nix::libc::STDOUT_FILENO)?;
                                if our_cols != cols || our_rows != rows {
                                    server_ctrl.send(protocol::Request::WindowSize {
                                        id,
                                        rows: our_rows,
                                        cols: our_cols
                                    }).await?;
                                }
                            }
                        }
                        _ => unreachable!("Server sent an unexpected event"),
                    }
                },
                Some(nbytes) = OptionFuture::from(data_channel.as_mut().map(|c| c.read(&mut data_buf[..]))) => {
                    let nbytes = nbytes?;
                    stdout_tx.send(data_buf[..nbytes].to_vec()).unwrap();
                    total_received += nbytes;
                }
                _ = signal.recv() => {
                    let (cols, rows) = get_term_size(nix::libc::STDOUT_FILENO)?;
                    if let Some(id) = id {
                        server_ctrl.send(protocol::Request::WindowSize { id, rows, cols }).await?;
                    }
                }
                input = stdin_rx.recv() => {
                    let input = input.unwrap();
                    if let Some(data_channel) = &mut data_channel {
                        data_channel.write_all(input.as_slice()).await?;
                    }
                }
            }
        };
        let res = server_ctrl.into_inner().shutdown().await;
        #[cfg(not(target_os = "macos"))]
        res.unwrap();
        #[cfg(target_os = "macos")]
        let _ = res;
        tracing::trace!("Connection closed");
        // Drain data_channel
        if let Some(mut data_channel) = data_channel {
            loop {
                let nbytes = data_channel.read(&mut data_buf[..]).await?;
                stdout_tx.send(data_buf[..nbytes].to_vec()).unwrap();
                total_received += nbytes;
                if nbytes == 0 {
                    break
                }
            }
        }
        drop(stdout_tx);
        stdout_thread.join().unwrap();

        drop(_guard);

        tracing::debug!("Connection closed, total received: {total_received}");
        Ok(status)
    }
}
