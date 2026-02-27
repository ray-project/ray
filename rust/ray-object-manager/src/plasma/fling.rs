// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! File descriptor passing over Unix domain sockets via `SCM_RIGHTS`.
//!
//! Replaces `src/ray/object_manager/plasma/fling.h/cc`.
//!
//! # Safety
//!
//! Uses `unsafe` for the low-level cmsg/sendmsg/recvmsg operations required
//! by `SCM_RIGHTS` FD passing. This is inherently unsafe but necessary for
//! the plasma shared-memory protocol.

use std::io;
use std::os::fd::RawFd;

/// Send a file descriptor over a Unix domain socket connection.
///
/// Uses `sendmsg` with `SCM_RIGHTS` ancillary data.
///
/// # Safety
/// `conn` and `fd` must be valid file descriptors.
pub fn send_fd(conn: RawFd, fd: RawFd) -> io::Result<()> {
    use libc::{
        c_void, cmsghdr, iovec, msghdr, sendmsg, CMSG_DATA, CMSG_FIRSTHDR, CMSG_LEN, CMSG_SPACE,
        SCM_RIGHTS, SOL_SOCKET,
    };
    use std::mem;
    use std::ptr;

    // We send a single byte as the payload (required for sendmsg)
    let mut buf: [u8; 1] = [0];
    let mut iov = iovec {
        iov_base: buf.as_mut_ptr() as *mut c_void,
        iov_len: 1,
    };

    // Ancillary data buffer for one file descriptor
    let cmsg_space = unsafe { CMSG_SPACE(mem::size_of::<RawFd>() as u32) } as usize;
    let mut cmsg_buf = vec![0u8; cmsg_space];

    let mut msg: msghdr = unsafe { mem::zeroed() };
    msg.msg_iov = &mut iov;
    msg.msg_iovlen = 1;
    msg.msg_control = cmsg_buf.as_mut_ptr() as *mut c_void;
    msg.msg_controllen = cmsg_space as _;

    // Fill in the control message header
    let cmsg: &mut cmsghdr = unsafe {
        let ptr = CMSG_FIRSTHDR(&msg);
        if ptr.is_null() {
            return Err(io::Error::other("CMSG_FIRSTHDR returned null"));
        }
        &mut *ptr
    };
    cmsg.cmsg_level = SOL_SOCKET;
    cmsg.cmsg_type = SCM_RIGHTS;
    cmsg.cmsg_len = unsafe { CMSG_LEN(mem::size_of::<RawFd>() as u32) } as _;

    // Copy the file descriptor into the control message data
    unsafe {
        ptr::copy_nonoverlapping(
            &fd as *const RawFd as *const u8,
            CMSG_DATA(cmsg),
            mem::size_of::<RawFd>(),
        );
    }

    let ret = unsafe { sendmsg(conn, &msg, 0) };
    if ret < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}

/// Receive a file descriptor from a Unix domain socket connection.
///
/// Uses `recvmsg` with `SCM_RIGHTS` ancillary data.
///
/// # Safety
/// `conn` must be a valid file descriptor for a Unix domain socket.
pub fn recv_fd(conn: RawFd) -> io::Result<RawFd> {
    use libc::{
        c_void, cmsghdr, iovec, msghdr, recvmsg, CMSG_DATA, CMSG_FIRSTHDR, CMSG_LEN, CMSG_SPACE,
        SCM_RIGHTS, SOL_SOCKET,
    };
    use std::mem;
    use std::ptr;

    let mut buf: [u8; 1] = [0];
    let mut iov = iovec {
        iov_base: buf.as_mut_ptr() as *mut c_void,
        iov_len: 1,
    };

    let cmsg_space = unsafe { CMSG_SPACE(mem::size_of::<RawFd>() as u32) } as usize;
    let mut cmsg_buf = vec![0u8; cmsg_space];

    let mut msg: msghdr = unsafe { mem::zeroed() };
    msg.msg_iov = &mut iov;
    msg.msg_iovlen = 1;
    msg.msg_control = cmsg_buf.as_mut_ptr() as *mut c_void;
    msg.msg_controllen = cmsg_space as _;

    let ret = unsafe { recvmsg(conn, &mut msg, 0) };
    if ret < 0 {
        return Err(io::Error::last_os_error());
    }

    // Extract the file descriptor from the control message
    let cmsg: &cmsghdr = unsafe {
        let ptr = CMSG_FIRSTHDR(&msg);
        if ptr.is_null() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "No control message received",
            ));
        }
        &*ptr
    };

    if cmsg.cmsg_level != SOL_SOCKET || cmsg.cmsg_type != SCM_RIGHTS {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Unexpected control message type",
        ));
    }

    if cmsg.cmsg_len < unsafe { CMSG_LEN(mem::size_of::<RawFd>() as u32) } as _ {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Control message too short",
        ));
    }

    let mut fd: RawFd = -1;
    unsafe {
        ptr::copy_nonoverlapping(
            CMSG_DATA(cmsg),
            &mut fd as *mut RawFd as *mut u8,
            mem::size_of::<RawFd>(),
        );
    }

    if fd < 0 {
        Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Received invalid file descriptor",
        ))
    } else {
        Ok(fd)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fd_passing_roundtrip() {
        // Create a Unix socket pair
        let mut fds: [RawFd; 2] = [0; 2];
        let ret =
            unsafe { libc::socketpair(libc::AF_UNIX, libc::SOCK_STREAM, 0, fds.as_mut_ptr()) };
        assert_eq!(ret, 0, "socketpair failed");

        // Create a temp file to get an FD to send
        let tmp = tempfile::tempfile().unwrap();
        let send_fd_val = {
            use std::os::fd::AsRawFd;
            tmp.as_raw_fd()
        };

        // Send the FD over the socket pair
        send_fd(fds[0], send_fd_val).expect("send_fd failed");

        // Receive it on the other end
        let received_fd = recv_fd(fds[1]).expect("recv_fd failed");
        assert!(received_fd >= 0);
        assert_ne!(received_fd, send_fd_val); // Should be a new FD number

        // Cleanup
        unsafe {
            libc::close(fds[0]);
            libc::close(fds[1]);
            libc::close(received_fd);
        }
    }
}
