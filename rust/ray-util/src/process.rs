// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! Process management utilities.
//!
//! Replaces C++ `process.cc/h`, `process_utils.cc/h`, `subreaper.cc/h`.

use std::process::Command;

/// Check if a process with the given PID is alive.
pub fn is_process_alive(pid: u32) -> bool {
    // On Unix, sending signal 0 checks process existence without killing it.
    #[cfg(unix)]
    {
        use nix::sys::signal;
        use nix::unistd::Pid;
        signal::kill(Pid::from_raw(pid as i32), None).is_ok()
    }
    #[cfg(not(unix))]
    {
        let _ = pid;
        false
    }
}

/// Kill a process by PID.
pub fn kill_process(pid: u32) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        use nix::sys::signal::{self, Signal};
        use nix::unistd::Pid;
        signal::kill(Pid::from_raw(pid as i32), Signal::SIGKILL)
            .map_err(std::io::Error::other)
    }
    #[cfg(not(unix))]
    {
        let _ = pid;
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "kill not supported on this platform",
        ))
    }
}

/// Get the current process ID.
pub fn get_pid() -> u32 {
    std::process::id()
}

/// Spawn a subprocess and return its PID.
pub fn spawn_process(
    executable: &str,
    args: &[&str],
    env: &[(&str, &str)],
) -> std::io::Result<u32> {
    let mut cmd = Command::new(executable);
    cmd.args(args);
    for (k, v) in env {
        cmd.env(k, v);
    }
    let child = cmd.spawn()?;
    Ok(child.id())
}

/// Set the current process as a subreaper (Linux only).
/// This ensures orphaned child processes are re-parented to this process.
#[cfg(target_os = "linux")]
pub fn set_subreaper() -> std::io::Result<()> {
    use libc::{prctl, PR_SET_CHILD_SUBREAPER};
    let ret = unsafe { prctl(PR_SET_CHILD_SUBREAPER, 1, 0, 0, 0) };
    if ret == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error())
    }
}

#[cfg(not(target_os = "linux"))]
pub fn set_subreaper() -> std::io::Result<()> {
    // Subreaper is Linux-specific; no-op on other platforms.
    Ok(())
}
