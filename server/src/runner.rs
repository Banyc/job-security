use std::os::{
    fd::{AsRawFd, FromRawFd, OwnedFd, RawFd},
    unix::process::CommandExt,
};

use futures_util::{SinkExt, StreamExt};
use nix::{
    sys::wait::{WaitPidFlag, WaitStatus},
    unistd::Pid,
};
use protocol::{ExitStatus, ProcessState};
use tokio::{io::AsyncWriteExt, net::UnixStream, select};

use crate::{runner_codec, RunnerEvent, RunnerRequest};

#[derive(Default, Debug, PartialEq, Eq)]
enum State {
    Stopped,
    #[default]
    Running,
    Resuming,
}

#[derive(Default)]
pub struct Runner {
    state: State,
}

fn set_foreground(fd: RawFd, foreground: Pid) -> nix::Result<()> {
    use nix::sys::signal;
    let mut sigset = signal::SigSet::empty();
    //sigset.add(signal::Signal::SIGTSTP);
    sigset.add(signal::Signal::SIGTTOU);
    //sigset.add(signal::Signal::SIGTTIN);
    //sigset.add(signal::Signal::SIGCHLD);
    signal::sigprocmask(signal::SigmaskHow::SIG_BLOCK, Some(&sigset), None)?;
    nix::unistd::tcsetpgrp(fd, foreground)?;

    let sigset = signal::SigSet::empty();
    signal::sigprocmask(signal::SigmaskHow::SIG_SETMASK, Some(&sigset), None)?;
    Ok(())
}

impl Runner {
    /// # Safety
    ///
    /// caller must ensure a valid file descriptor is passed in
    /// via the RUNNER_FD environment variable.
    pub async unsafe fn run(self) {
        let fd = OwnedFd::from_raw_fd(std::env::var("RUNNER_FD").unwrap().parse::<i32>().unwrap());
        let file: UnixStream = std::os::unix::net::UnixStream::from(fd).try_into().unwrap();
        self.real_run(file).await;
    }

    async fn real_run(mut self, ctl: UnixStream) {
        // Reset relevant signal handlers
        use nix::sys::signal;
        unsafe {
            signal::signal(signal::Signal::SIGTSTP, signal::SigHandler::SigDfl).unwrap();
            signal::signal(signal::Signal::SIGTTOU, signal::SigHandler::SigDfl).unwrap();
            signal::signal(signal::Signal::SIGTTIN, signal::SigHandler::SigDfl).unwrap();
            let sigset = signal::SigSet::empty();
            signal::sigprocmask(signal::SigmaskHow::SIG_SETMASK, Some(&sigset), None).unwrap();
        }

        let mut framed = tokio_util::codec::Framed::new(ctl, runner_codec());
        let RunnerRequest::Start { command, env, pwd } = framed.next().await.unwrap().unwrap()
        else {
            panic!();
        };
        let mut cmd = std::process::Command::new(&command[0]);
        cmd.args(&command[1..]).current_dir(&pwd).envs(env);
        unsafe {
            cmd.pre_exec(|| {
                nix::unistd::setpgid(Pid::from_raw(0), Pid::from_raw(0))?;
                set_foreground(std::io::stdin().as_raw_fd(), nix::unistd::getpid())?;
                Ok(())
            });
        }
        let child = cmd.spawn().unwrap();
        let pid = child.id();
        tracing::debug!("pid: {}", pid);
        let mut signal =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::child()).unwrap();
        nix::unistd::setpgid(Pid::from_raw(pid as i32), Pid::from_raw(pid as i32)).ok();
        framed
            .send(RunnerEvent::StateChanged(ProcessState::Running, pid))
            .await
            .unwrap();
        loop {
            select! {
                _ = signal.recv() => {
                    let status = nix::sys::wait::waitpid(
                        Pid::from_raw(pid as i32),
                        Some(WaitPidFlag::WUNTRACED | WaitPidFlag::WNOHANG)
                    );
                    let status = match status {
                        Ok(status) => status,
                        Err(nix::errno::Errno::EAGAIN) => continue,
                        Err(e) => panic!("{}", e),
                    };
                    match status {
                        WaitStatus::Exited(_, ec) => {
                            framed.send(
                                RunnerEvent::StateChanged(
                                    ProcessState::Terminated(ExitStatus::Exited(ec)),
                                    pid
                                )
                            ).await.unwrap();
                            break;
                        },
                        WaitStatus::Signaled(_, sig, _) => {
                            framed.send(
                                RunnerEvent::StateChanged(
                                    ProcessState::Terminated(ExitStatus::Signaled(sig as i32)),
                                    pid
                                )
                            ).await.unwrap();
                            break;
                        },
                        WaitStatus::Stopped(_, _) => {
                            self.state = State::Stopped;
                            set_foreground(
                                std::io::stdin().as_raw_fd(),
                                nix::unistd::getpid()
                            ).unwrap();
                            framed.send(
                                RunnerEvent::StateChanged(ProcessState::Stopped, pid)
                            ).await.unwrap();
                        },
                        WaitStatus::StillAlive => {
                            assert_eq!(self.state, State::Resuming);
                            self.state = State::Running;
                            framed.send(
                                RunnerEvent::StateChanged(ProcessState::Running, pid)
                            ).await.unwrap();
                        },
                        x => unreachable!("{x:?}"),
                    }
                },
                req = framed.next() => {
                    let req = req.unwrap().unwrap();
                    match req {
                        RunnerRequest::Resume => {
                            assert_eq!(self.state, State::Stopped);
                            set_foreground(
                                std::io::stdin().as_raw_fd(),
                                Pid::from_raw(pid as i32)
                            ).unwrap();
                            nix::sys::signal::kill(
                                Pid::from_raw(pid as i32),
                                nix::sys::signal::Signal::SIGCONT
                            ).unwrap();
                            self.state = State::Resuming;
                            #[cfg(target_os = "macos")]
                            {
                                // `signal.recv()` will NOT be triggered at macOS so we skip it and assume the process state has been back to running
                                self.state = State::Running;
                                framed.send(
                                    RunnerEvent::StateChanged(ProcessState::Running, pid)
                                ).await.unwrap();
                            }
                        },
                        RunnerRequest::Start { .. } => {
                            panic!();
                        }
                    }
                },
            }
        }
        framed.get_mut().flush().await.unwrap();
    }
}
