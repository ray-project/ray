// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! Networking utilities.
//!
//! Replaces C++ `network_util.cc/h` and `port_persistence.cc/h`.

use std::net::{IpAddr, Ipv4Addr, SocketAddr, TcpListener, UdpSocket};

/// Get the local IP address by connecting to a public DNS server.
/// This is the same approach as the C++ `GetNodeIpAddress`.
pub fn get_local_ip() -> IpAddr {
    // Connect UDP socket to Google DNS to determine local interface address
    let socket = UdpSocket::bind("0.0.0.0:0").expect("Failed to bind UDP socket");
    match socket.connect("8.8.8.8:53") {
        Ok(()) => socket
            .local_addr()
            .map(|addr| addr.ip())
            .unwrap_or(IpAddr::V4(Ipv4Addr::LOCALHOST)),
        Err(_) => IpAddr::V4(Ipv4Addr::LOCALHOST),
    }
}

/// Find an available TCP port on the given address.
/// Returns 0 if no port could be found.
pub fn get_free_port(addr: IpAddr) -> u16 {
    TcpListener::bind(SocketAddr::new(addr, 0))
        .and_then(|l| l.local_addr())
        .map(|a| a.port())
        .unwrap_or(0)
}

/// Check if a TCP port is available on the given address.
pub fn is_port_available(addr: IpAddr, port: u16) -> bool {
    TcpListener::bind(SocketAddr::new(addr, port)).is_ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_free_port() {
        let port = get_free_port(IpAddr::V4(Ipv4Addr::LOCALHOST));
        assert!(port > 0);
        assert!(is_port_available(IpAddr::V4(Ipv4Addr::LOCALHOST), port));
    }
}
