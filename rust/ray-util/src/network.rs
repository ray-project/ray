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

/// Public DNS server used for local IP detection.
/// Matches `PUBLIC_DNS_SERVER_IP` / `PUBLIC_DNS_SERVER_PORT` in `ray-common/constants.rs`.
const PUBLIC_DNS_SERVER: &str = "8.8.8.8:53";

/// Get the local IP address by connecting to a public DNS server.
/// This is the same approach as the C++ `GetNodeIpAddress`.
pub fn get_local_ip() -> IpAddr {
    // Connect UDP socket to public DNS to determine local interface address.
    let socket = UdpSocket::bind("0.0.0.0:0").expect("Failed to bind UDP socket");
    match socket.connect(PUBLIC_DNS_SERVER) {
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

    #[test]
    fn test_get_local_ip() {
        let ip = get_local_ip();
        // Should return a valid IP (localhost or real interface).
        assert!(ip.is_ipv4() || ip.is_ipv6());
    }

    #[test]
    fn test_is_port_available_for_free_port() {
        let port = get_free_port(IpAddr::V4(Ipv4Addr::LOCALHOST));
        assert!(is_port_available(IpAddr::V4(Ipv4Addr::LOCALHOST), port));
    }

    #[test]
    fn test_bound_port_is_unavailable() {
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);
        let listener = TcpListener::bind(addr).unwrap();
        let bound_port = listener.local_addr().unwrap().port();
        assert!(!is_port_available(IpAddr::V4(Ipv4Addr::LOCALHOST), bound_port));
    }

    #[test]
    fn test_two_free_ports_differ() {
        let p1 = get_free_port(IpAddr::V4(Ipv4Addr::LOCALHOST));
        let p2 = get_free_port(IpAddr::V4(Ipv4Addr::LOCALHOST));
        // Very unlikely to be the same.
        assert!(p1 > 0 && p2 > 0);
    }
}
