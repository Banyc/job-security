mod pty;
use std::{
    ffi::{OsStr, OsString},
    os::fd::{AsFd, AsRawFd, OwnedFd},
    path::Path,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use dlv_list::VecList;
use futures_util::{future::OptionFuture, stream::FusedStream, Sink, SinkExt, StreamExt};
use protocol::{
    bytes::{Buf, BufMut, Bytes, BytesMut},
    MapCodec, ProcessState,
};
use pty::PtyProcess;
use serde::{Deserialize, Serialize};
use tokio::{
    io::AsyncWriteExt,
    net::{UnixListener, UnixStream},
    select,
    sync::{
        mpsc::{Receiver, Sender},
        RwLock,
    },
};
use tokio_command_fds::{CommandFdExt, FdMapping};
use tokio_util::codec::{Encoder, Framed};
use withfd::{WithFd, WithFdExt};

use crate::util::OptionIo;

mod runner;
mod util;

pub use runner::Runner;

#[derive(Debug, Serialize, Deserialize)]
enum RunnerRequest {
    Start {
        command: Vec<OsString>,
        env:     Vec<(OsString, OsString)>,
        pwd:     std::path::PathBuf,
    },
    Resume,
}

#[derive(Debug, Serialize, Deserialize)]
enum RunnerEvent {
    StateChanged(ProcessState, u32),
}

fn runner_codec() -> impl tokio_util::codec::Encoder<RunnerEvent, Error = std::io::Error>
       + tokio_util::codec::Decoder<Item = RunnerRequest, Error = std::io::Error> {
    let codec = tokio_util::codec::length_delimited::LengthDelimitedCodec::new();
    MapCodec::new(
        codec,
        |bytes: BytesMut| bincode::deserialize(&bytes).unwrap(),
        |request| -> Bytes { bincode::serialize(&request).unwrap().into() },
    )
}

fn server_codec() -> impl tokio_util::codec::Encoder<RunnerRequest, Error = std::io::Error>
       + tokio_util::codec::Decoder<Item = RunnerEvent, Error = std::io::Error> {
    let codec = tokio_util::codec::length_delimited::LengthDelimitedCodec::new();
    MapCodec::new(
        codec,
        |bytes: BytesMut| bincode::deserialize(&bytes).unwrap(),
        |event| -> Bytes { bincode::serialize(&event).unwrap().into() },
    )
}

struct ClientChannel {
    ctrl: Framed<WithFd<UnixStream>, protocol::DynServerCodec>,
    data: UnixStream,
}

struct ProcessShared {
    // Some contracts to keep the server race-safe:
    //
    // - reaped can only be set to true when client_connected is true.
    // - only one client can cause client_connected to become true. to avoid multiple clients
    //   connect to the same job.
    // - key can only be removed from the lru list when reaped is true. because otherwise the
    //   handle_client could have picked the job the resume (because the job's reaped is false),
    //   just before the job removes itself from the lru.
    pid:              u32,
    key:              dlv_list::Index<u32>,
    command:          Vec<OsString>,
    state:            RwLock<ProcessState>,
    reaped:           AtomicBool,
    client_connected: AtomicBool,
    pty:              PtyProcess,
    size:             RwLock<(u16, u16)>,
}

struct Process {
    /// State shared between the server and the runner
    runner_shared: Arc<ProcessShared>,
    ctrl_tx:       Pin<Box<dyn Sink<RunnerRequest, Error = std::io::Error> + Send + Sync>>,
    new_client:    Sender<(ClientChannel, bool)>,
}

pub struct Server {
    /// Listener of the control channel between clients and this server
    listener:    std::os::unix::net::UnixListener,
    /// Arguments to spawn a runner
    ///
    /// When a runner is needed, `Server` will exec `/proc/self/exe` with
    /// `runner_args` as argument.
    runner_args: Vec<OsString>,
    /// Exclusive lock for the existence of this server
    lock:        FlockGuard,
}

struct FlockGuard {
    fd: std::fs::File,
}

impl FlockGuard {
    fn new(fd: std::fs::File) -> nix::Result<Self> {
        nix::fcntl::flock(fd.as_raw_fd(), nix::fcntl::FlockArg::LockExclusiveNonblock)?;
        Ok(Self { fd })
    }
}

impl Drop for FlockGuard {
    fn drop(&mut self) {
        nix::fcntl::flock(self.fd.as_raw_fd(), nix::fcntl::FlockArg::Unlock).unwrap();
    }
}

fn next_free_slot(v: &mut Vec<Option<Process>>) -> (usize, &mut Option<Process>) {
    if let Some((i, _)) = v.iter().enumerate().find(|(_, slot)| {
        slot.as_ref()
            .map(|slot| slot.runner_shared.reaped.load(Ordering::Relaxed))
            .unwrap_or(true)
    }) {
        (i, &mut v[i])
    } else {
        let id = v.len();
        v.push(None);
        (id, v.last_mut().unwrap())
    }
}
impl Server {
    pub fn new(runner_args: Vec<OsString>) -> std::io::Result<Self> {
        let uid = nix::unistd::getuid();
        let runtime_dir = Path::new(protocol::BASE_DIR)
            .join(uid.to_string())
            .join("job-security");
        std::fs::create_dir_all(&runtime_dir)?;
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(runtime_dir.join("lock"))?;
        let lock = match FlockGuard::new(file) {
            Ok(lock) => lock,
            Err(nix::errno::Errno::EWOULDBLOCK) =>
                return Err(std::io::Error::new(
                    std::io::ErrorKind::AlreadyExists,
                    "another instance is running",
                )),
            Err(err) => return Err(err.into()),
        };

        std::fs::remove_file(runtime_dir.join("sock")).ok();
        let listener = std::os::unix::net::UnixListener::bind(runtime_dir.join("sock"))?;
        Ok(Self {
            listener,
            runner_args,
            lock,
        })
    }

    /// Fork as a runner child from the server
    ///
    /// The runner then runs the command
    #[allow(clippy::type_complexity)]
    fn start_process(
        command: &Vec<OsString>,
        rows: u16,
        cols: u16,
    ) -> (
        PtyProcess,
        Pin<Box<dyn Sink<RunnerRequest, Error = std::io::Error> + Send + Sync>>,
        Pin<Box<dyn FusedStream<Item = std::io::Result<RunnerEvent>> + Send + Sync>>,
    ) {
        let pipe = tokio::net::UnixStream::pair().unwrap();
        let exe = palaver::env::exe_path().unwrap();
        let mut cmd = tokio::process::Command::new(exe);
        cmd.args(command)
            .env("RUNNER_FD", "3")
            .fd_mappings(vec![FdMapping {
                parent_fd: pipe.0.as_raw_fd(),
                child_fd:  3,
            }])
            .unwrap();

        let (pty, _) = PtyProcess::spawn(cmd, rows, cols).unwrap();
        let framed = tokio_util::codec::Framed::new(pipe.1, server_codec());
        let (tx, rx) = framed.split();
        (pty, Box::pin(tx), Box::pin(rx.fuse()))
    }

    /// Relay message between zero or one client and a runner
    ///
    /// `shared`: shared state between this server and the runner
    async fn handle_runner_channels(
        id: u32,
        shared: Arc<ProcessShared>,
        lru: Arc<RwLock<VecList<u32>>>,
        mut new_client_rx: Receiver<(ClientChannel, bool)>,
        mut runner_ctrl_rx: Pin<
            Box<dyn FusedStream<Item = std::io::Result<RunnerEvent>> + Send + Sync>,
        >,
    ) -> std::io::Result<()> {
        use tokio::io::AsyncReadExt;
        let pty: OwnedFd = shared.pty.get_raw_handle().unwrap().into();
        tracing::debug!("fd dup'd to {}", pty.as_raw_fd());
        let mut pty_write_buf = BytesMut::with_capacity(1024);
        let flags = nix::fcntl::fcntl(pty.as_raw_fd(), nix::fcntl::FcntlArg::F_GETFL).unwrap();
        let flags = nix::fcntl::OFlag::from_bits_truncate(flags) | nix::fcntl::OFlag::O_NONBLOCK;
        nix::fcntl::fcntl(pty.as_raw_fd(), nix::fcntl::FcntlArg::F_SETFL(flags)).unwrap();
        let pty = tokio::io::unix::AsyncFd::new(pty).unwrap();
        let mut pty_read_buf = [0u8; 1024];
        let mut client_read_buf = [0u8; 1024];
        let mut client_write_buf = BytesMut::with_capacity(1024);
        let mut client_ctrl: Option<Framed<WithFd<UnixStream>, protocol::DynServerCodec>> = None;
        let (mut client_read, mut client_write) =
            tokio::io::split(OptionIo::<UnixStream>::default());
        let mut verdict = ProcessState::Running;

        // Handle client and/or runner messages until we are reaped.
        // We will only be reaped when all of the following are true:
        //  - the client is connected
        //  - the client_write_buf is empty && pty_read_finished is true
        //  - the runner finished
        //
        // We also want to send the final process state to the client after we have
        // flushed the client_write_buf, so we only send that after we are out
        // of the while loop.
        let mut pty_read_finished = false;
        while !shared.reaped.load(Ordering::Relaxed) {
            let mut disconnect_client = select! {
                ready = pty.readable(), if !pty_read_finished => {
                    let mut ready = ready?;
                    if let Ok(nbytes) = ready.try_io(|inner| {
                        let nbytes = nix::unistd::read(inner.get_ref().as_raw_fd(), &mut pty_read_buf[..])?;
                        if nbytes == 0 {
                            return Err(std::io::ErrorKind::WouldBlock.into());
                        }
                        Ok(nbytes)
                    }) {
                        if let Ok(nbytes) = nbytes {
                            client_write_buf.reserve(nbytes);
                            client_write_buf.put_slice(&pty_read_buf[..nbytes]);
                        } else {
                            tracing::info!("Read everything from pty, read resulted in error.");
                            pty_read_finished = true;
                        }
                    }
                    false
                },
                ready = pty.writable(), if !pty_write_buf.is_empty() && !verdict.is_terminated()=> {
                    let mut ready = ready?;
                    if let Ok(nbytes) = ready.try_io(|inner| {
                        let nbytes = nix::unistd::write(inner.get_ref().as_raw_fd(), &pty_write_buf[..])?;
                        if nbytes == 0 {
                            return Err(std::io::ErrorKind::WouldBlock.into());
                        }
                        Ok(nbytes)
                    }) {
                        let nbytes = nbytes?;
                        tracing::debug!("Sent {nbytes} bytes to the pty");
                        pty_write_buf.advance(nbytes);
                    }
                    false
                },
                nbytes = client_write.write(&client_write_buf[..]), if !client_write_buf.is_empty() => {
                    match nbytes {
                        Ok(0) | Err(_) => true,
                        Ok(nbytes) => {
                            tracing::debug!("Sent {nbytes} bytes to the client");
                            client_write_buf.advance(nbytes);
                            false
                        }
                    }
                },
                nbytes = client_read.read(&mut client_read_buf[..]) => {
                    tracing::debug!("client read {nbytes:?}");
                    match nbytes {
                        Ok(0) | Err(_) => true,
                        Ok(nbytes) => {
                            pty_write_buf.reserve(nbytes);
                            pty_write_buf.put_slice(&client_read_buf[..nbytes]);
                            false
                        }
                    }
                },
                new_client = new_client_rx.recv() => {
                    assert!(client_ctrl.is_none());
                    let (ClientChannel { ctrl, data }, with_output) = new_client.unwrap();
                    tracing::info!("New client connected, {with_output}");
                    client_ctrl = Some(ctrl);
                    (client_read, client_write) = tokio::io::split(Some(data).into());
                    if !with_output {
                        client_write_buf.clear();
                    }
                    false
                },
                req = OptionFuture::from(client_ctrl.as_mut().map(|e| e.next())), if client_ctrl.is_some() => {
                    let req = req.unwrap();
                    if let Some(Ok(req)) = req {
                        if let protocol::Request::WindowSize { rows, cols, .. } = req {
                            tracing::info!("Setting window size to {rows}x{cols}");
                            shared.pty.set_window_size(cols, rows).unwrap();
                        } else {
                            client_ctrl.as_mut()
                                .unwrap()
                                .send(protocol::Event::Error(protocol::Error::InvalidRequest))
                                .await?;
                        }
                        false
                    } else {
                        true
                    }
                },
                e = runner_ctrl_rx.next(), if !runner_ctrl_rx.is_terminated() => match e {
                    Some(Ok(RunnerEvent::StateChanged(e, _))) => {
                        tracing::info!("New runner state: {e:?}");
                        *shared.state.write().await = e;
                        match e {
                            ProcessState::Stopped => {
                                // New process state is Stopped.

                                if let Some(client_ctrl) = &mut client_ctrl {
                                    // We are going to detach the client, so try to flush client_write_buf first.
                                    //
                                    // this is best effort, because the client could disconnect as we try
                                    // to send data.
                                    let mut client_data = client_read.unsplit(client_write).into_inner().unwrap();
                                    while !client_write_buf.is_empty() {
                                        match client_data.write(&client_write_buf[..]).await {
                                            Ok(0) | Err(_) => break,
                                            Ok(n) => {
                                                client_write_buf.advance(n);
                                            }
                                        }
                                    }

                                    tracing::info!("Sending Stopped to the client");
                                    let _ = client_ctrl.send(protocol::Event::StateChanged {
                                        id, state: ProcessState::Stopped
                                    }).await;
                                    let _ = client_ctrl.flush().await;
                                }

                                // Fix use of partially move client_read/client_write
                                (client_read, client_write) = tokio::io::split(None.into());

                                let mut lru = lru.write().await;
                                let head = lru.indices().next().unwrap();
                                if head != shared.key {
                                    lru.move_before(shared.key, head);
                                }
                                true
                            },
                            ProcessState::Terminated(_) => {
                                verdict = e;
                                false
                            },
                            ProcessState::Running => {
                                if let Some(client_ctrl) = &mut client_ctrl {
                                    let (cols, rows) = *shared.size.read().await;
                                    client_ctrl.send(protocol::Event::WindowSize { cols, rows }).await.is_err()
                                } else {
                                    false
                                }
                            },
                        }
                    },
                    Some(Err(e)) => {
                        tracing::warn!("Error: {:?}", e);
                        return Err(e)
                    },
                    None => {
                        tracing::info!("Runner disconnected");
                        assert!(verdict.is_terminated(), "Runner terminated unexpectedly");
                        false
                    }
                }
            };
            if verdict.is_terminated() &&
                client_write_buf.is_empty() &&
                shared.client_connected.load(Ordering::Acquire)
            {
                if pty
                    .poll_read_ready(&mut std::task::Context::from_waker(
                        futures_util::task::noop_waker_ref(),
                    ))
                    .is_pending()
                {
                    tracing::info!("Read everything from pty");
                    pty_read_finished = true;
                }
                if pty_read_finished {
                    // The job has terminated, and we have read and sent all the output data.
                    // Now try to send the final verdict, if successful, we can reap the process
                    let mut client_event = client_ctrl.take().unwrap();
                    tracing::info!("Sending final process state {verdict:?} to the client");
                    let sent = client_event
                        .send(protocol::Event::StateChanged { id, state: verdict })
                        .await
                        .is_ok();
                    let sent = sent && client_event.flush().await.is_ok();
                    if sent {
                        shared.reaped.store(true, Ordering::Relaxed);
                    } // else {
                      //    The client disconnected, we keep waiting until the client reconnects.
                      // }

                    // either way, we disconnect the client for now
                    disconnect_client = true;
                }
            }
            if disconnect_client {
                tracing::info!(
                    "disconnecting the client, was connected: {}",
                    shared.client_connected.load(Ordering::Relaxed)
                );
                (client_read, client_write) = tokio::io::split(None.into());
                client_ctrl = None;
                shared.client_connected.store(false, Ordering::Release);
            }
        }

        // The job has been reaped, remove it from the list
        lru.write().await.remove(shared.key).unwrap();
        tracing::info!("Runner task finished");
        Ok(())
    }

    async fn setup_client_server_data_channel(
        id: u32,
        slot: &Process,
        mut ctrl_tx: Framed<WithFd<UnixStream>, protocol::DynServerCodec>,
        with_output: bool,
    ) {
        // Send Started event with the file descriptor
        let mut buf = BytesMut::new();
        let data_channels = tokio::net::UnixStream::pair().unwrap();
        ctrl_tx
            .codec_mut()
            .encode(
                protocol::Event::StateChanged {
                    id,
                    state: ProcessState::Running,
                },
                &mut buf,
            )
            .unwrap();
        let Ok(_) = ctrl_tx
            .get_mut()
            .write_with_fd(&buf, &[data_channels.0.as_fd()])
            .await
        else {
            // The client could have disconnected, which is fine. Don't send the client in
            // this case. Also set client_connected to false
            slot.runner_shared
                .client_connected
                .store(false, Ordering::Relaxed);
            return;
        };
        slot.new_client
            .send((
                ClientChannel {
                    data: data_channels.1,
                    ctrl: ctrl_tx,
                },
                with_output,
            ))
            .await
            .map_err(|_| {
                std::io::Error::new(
                    std::io::ErrorKind::BrokenPipe,
                    "Client task has stopped unexpectedly",
                )
            })
            .unwrap();
    }

    async fn handle_new_client(
        stream: tokio::net::UnixStream,
        processes: Arc<RwLock<Vec<Option<Process>>>>,
        lru: Arc<RwLock<VecList<u32>>>,
        quit: tokio::sync::mpsc::Sender<()>,
        runner_args: Arc<Vec<OsString>>,
    ) -> std::io::Result<()> {
        let codec = Box::new(protocol::server_codec())
            as Box<
                dyn protocol::Codec<protocol::Event, protocol::Request, std::io::Error>
                    + Send
                    + Sync
                    + Unpin,
            >;
        let mut client_ctrl = tokio_util::codec::Framed::new(stream.with_fd(), codec);

        let Some(request) = client_ctrl.next().await else {
            return Ok(())
        };
        let request = request?;
        let new_client = tokio::sync::mpsc::channel(1);
        match request {
            protocol::Request::Start {
                command,
                env,
                pwd,
                rows,
                cols,
            } => {
                let mut processes = processes.write().await;
                let (pty, mut runner_ctrl_tx, mut runner_ctrl_rx) =
                    Self::start_process(&runner_args, rows, cols);
                let (id, slot) = next_free_slot(&mut processes);
                let id = id as u32;
                let index = lru.write().await.push_back(id);
                runner_ctrl_tx
                    .send(RunnerRequest::Start {
                        command: command.clone(),
                        env,
                        pwd,
                    })
                    .await?;
                let x = runner_ctrl_rx.next().await;
                let Some(Ok(RunnerEvent::StateChanged(ProcessState::Running, pid))) = &x else {
                    panic!("{x:?}")
                };
                let runner_shared = Arc::new(ProcessShared {
                    pid: *pid,
                    key: index,
                    command,
                    state: RwLock::new(ProcessState::Running),
                    reaped: false.into(),
                    client_connected: AtomicBool::new(true),
                    pty,
                    size: RwLock::new((cols, rows)),
                });
                let slot = slot.insert(Process {
                    runner_shared: runner_shared.clone(),
                    ctrl_tx:       runner_ctrl_tx,
                    new_client:    new_client.0,
                });

                Self::setup_client_server_data_channel(id, slot, client_ctrl, false).await;

                tokio::spawn(async move {
                    Self::handle_runner_channels(
                        id,
                        runner_shared,
                        lru.clone(),
                        new_client.1,
                        runner_ctrl_rx,
                    )
                    .await
                    .unwrap()
                });
            },
            protocol::Request::Resume { id, with_output } => {
                use protocol::{Error, Event};
                let processes_read = processes.read().await;
                let id = match id {
                    Some(id) => Some(id),
                    None => {
                        let lru = lru.read().await;
                        let mut latest = None;
                        for id in lru.iter().copied() {
                            let Some(slot) = processes_read[id as usize].as_ref() else {
                                continue
                            };
                            if !slot.runner_shared.client_connected.load(Ordering::Acquire) &&
                                !slot.runner_shared.reaped.load(Ordering::Relaxed)
                            {
                                latest = Some(id);
                            }
                        }
                        latest
                    },
                };
                let Some(id) = id else {
                    tracing::info!("No process to resume");
                    client_ctrl
                        .send(Event::Error(Error::NotFound { id: None }))
                        .await?;
                    return Ok(())
                };
                let Some(slot) = processes_read
                    .get(id as usize)
                    .and_then(|slot| slot.as_ref())
                else {
                    tracing::info!("Process {id} not found");
                    client_ctrl
                        .send(Event::Error(Error::NotFound { id: Some(id) }))
                        .await?;
                    return Ok(())
                };
                if slot.runner_shared.reaped.load(Ordering::Relaxed) {
                    tracing::info!("Process {id} not found");
                    client_ctrl
                        .send(Event::Error(Error::NotFound { id: Some(id) }))
                        .await?;
                    return Ok(())
                }
                if slot
                    .runner_shared
                    .client_connected
                    .compare_exchange(false, true, Ordering::Relaxed, Ordering::Relaxed)
                    .is_err()
                {
                    tracing::info!("Process {id} already has a client");
                    client_ctrl
                        .send(Event::Error(Error::AlreadyConnected { id }))
                        .await?;
                    return Ok(())
                }
                // At most one client can reach this point
                let need_resuming = *slot.runner_shared.state.read().await == ProcessState::Stopped;
                drop(processes_read);

                // A stopped process cannot spontaneously resume, so we can safely assume even
                // after we dropped the processes read lock, need_resume is still up to date.
                let mut processes = processes.write().await;
                let slot = processes[id as usize].as_mut().unwrap();
                if need_resuming {
                    slot.ctrl_tx.send(RunnerRequest::Resume).await?;
                }
                tracing::info!("Client is reconnecting to job {id}");
                Self::setup_client_server_data_channel(id, slot, client_ctrl, with_output).await;
            },
            protocol::Request::WindowSize { .. } => {
                client_ctrl
                    .send(protocol::Event::Error(protocol::Error::InvalidRequest))
                    .await?;
            },
            protocol::Request::ListProcesses => {
                let processes = processes.read().await;
                for (id, process) in processes.iter().enumerate() {
                    let Some(process) = process else { continue };
                    let connected = process
                        .runner_shared
                        .client_connected
                        .load(Ordering::Acquire);
                    if process.runner_shared.reaped.load(Ordering::Relaxed) {
                        continue
                    }
                    let state = *process.runner_shared.state.read().await;
                    tracing::info!("Sending process {id} {state:?} {connected}");
                    client_ctrl
                        .send(protocol::Event::Process(protocol::Process {
                            command: process
                                .runner_shared
                                .command
                                .as_slice()
                                .join(OsStr::new(" ")),
                            pid: process.runner_shared.pid,
                            id: id as u32,
                            state,
                            connected,
                        }))
                        .await?;
                }
            },
            protocol::Request::Quit => {
                let _ = quit.send(()).await;
            },
        }
        Ok(())
    }

    pub async fn run(self) -> std::io::Result<()> {
        let Self {
            listener,
            runner_args,
            lock: _lock,
        } = self;
        listener.set_nonblocking(true)?;
        let listener: UnixListener = listener.try_into()?;
        let processes = Arc::new(RwLock::new(Vec::new()));
        let lru = Arc::new(RwLock::new(VecList::new()));
        let runner_args = Arc::new(runner_args);
        let (quit_handle, mut quit) = tokio::sync::mpsc::channel(1);
        let mut tasks = tokio::task::JoinSet::new();
        tasks.spawn(async move {
            let (_tx, rx) = tokio::sync::oneshot::channel::<()>();
            let _ = rx.await; // Block forever to make sure `tasks.join_next()` always return `Some`
            Ok(())
        });
        loop {
            select! {
                result = listener.accept() => {
                    let (stream, _) = result?;
                    tasks.spawn(Self::handle_new_client(
                        stream,
                        processes.clone(),
                        lru.clone(),
                        quit_handle.clone(),
                        runner_args.clone(),
                    ));
                },
                Some(res) = tasks.join_next() => {
                    res.expect("tokio thread panicked")?;
                }
                _ = quit.recv() => break,
            }
        }
        Ok(())
    }
}
