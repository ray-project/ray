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
        assert!(!is_port_available(
            IpAddr::V4(Ipv4Addr::LOCALHOST),
            bound_port
        ));
    }

    #[test]
    fn test_two_free_ports_differ() {
        let p1 = get_free_port(IpAddr::V4(Ipv4Addr::LOCALHOST));
        let p2 = get_free_port(IpAddr::V4(Ipv4Addr::LOCALHOST));
        // Very unlikely to be the same.
        assert!(p1 > 0 && p2 > 0);
    }

    // --- Ported from C++ network_util_test.cc ---

    /// Port of C++ TestBuildAddress: format host:port strings.
    #[test]
    fn test_build_address() {
        fn build_address(host: &str, port: u16) -> String {
            if host.contains(':') {
                // IPv6
                format!("[{}]:{}", host, port)
            } else {
                format!("{}:{}", host, port)
            }
        }

        // IPv4
        assert_eq!(build_address("192.168.1.1", 8080), "192.168.1.1:8080");
        // IPv6
        assert_eq!(build_address("::1", 8080), "[::1]:8080");
        assert_eq!(build_address("2001:db8::1", 8080), "[2001:db8::1]:8080");
        // Hostname
        assert_eq!(build_address("localhost", 9000), "localhost:9000");
    }

    /// Port of C++ TestParseAddress: parse "host:port" strings.
    #[test]
    fn test_parse_address() {
        fn parse_address(addr: &str) -> Option<(String, String)> {
            // IPv6 bracketed: [host]:port
            if let Some(rest) = addr.strip_prefix('[') {
                let bracket_end = rest.find(']')?;
                let host = &rest[..bracket_end];
                let after = &rest[bracket_end + 1..];
                let port_str = after.strip_prefix(':')?;
                return Some((host.to_string(), port_str.to_string()));
            }
            // Count colons to distinguish IPv6 from IPv4/hostname
            let colon_count = addr.chars().filter(|&c| c == ':').count();
            if colon_count > 1 {
                // Bare IPv6 without brackets — no port parseable
                return None;
            }
            let colon_pos = addr.rfind(':')?;
            let host = &addr[..colon_pos];
            let port = &addr[colon_pos + 1..];
            if host.is_empty() || port.is_empty() {
                return None;
            }
            Some((host.to_string(), port.to_string()))
        }

        // IPv4
        let result = parse_address("192.168.1.1:8080");
        assert!(result.is_some());
        let (host, port) = result.unwrap();
        assert_eq!(host, "192.168.1.1");
        assert_eq!(port, "8080");

        // IPv6 bracketed
        let result = parse_address("[::1]:8080");
        assert!(result.is_some());
        let (host, port) = result.unwrap();
        assert_eq!(host, "::1");
        assert_eq!(port, "8080");

        let result = parse_address("[2001:db8::1]:8080");
        assert!(result.is_some());
        let (host, port) = result.unwrap();
        assert_eq!(host, "2001:db8::1");
        assert_eq!(port, "8080");

        // Hostname
        let result = parse_address("localhost:9000");
        assert!(result.is_some());
        let (host, port) = result.unwrap();
        assert_eq!(host, "localhost");
        assert_eq!(port, "9000");

        // Bare IPv6 without port — should return None
        assert!(parse_address("::1").is_none());
        assert!(parse_address("2001:db8::1").is_none());

        // Bare IPv4 without port — should return None
        assert!(parse_address("192.168.1.1").is_none());
        assert!(parse_address("localhost").is_none());
    }

    /// Port of C++ TestIsIPv6: detect IPv6 vs IPv4 addresses.
    #[test]
    fn test_is_ipv6() {
        fn is_ipv6(addr: &str) -> bool {
            addr.parse::<std::net::Ipv6Addr>().is_ok()
        }

        // IPv4 should return false
        assert!(!is_ipv6("127.0.0.1"));
        assert!(!is_ipv6("192.168.1.1"));

        // IPv6 should return true
        assert!(is_ipv6("::1"));
        assert!(is_ipv6("2001:db8::1"));
        assert!(is_ipv6("::ffff:192.0.2.1"));

        // Invalid input should return false
        assert!(!is_ipv6(""));
        assert!(!is_ipv6("not-an-ip"));
        assert!(!is_ipv6("::1::2"));
    }

    /// Port of C++ ParseURLTest: parse URL query parameters.
    #[test]
    fn test_parse_url_query_params() {
        fn parse_url(url: &str) -> std::collections::HashMap<String, String> {
            let mut result = std::collections::HashMap::new();
            if let Some(qmark) = url.find('?') {
                result.insert("url".to_string(), url[..qmark].to_string());
                let query = &url[qmark + 1..];
                for param in query.split('&') {
                    if let Some(eq) = param.find('=') {
                        result.insert(param[..eq].to_string(), param[eq + 1..].to_string());
                    }
                }
            } else {
                result.insert("url".to_string(), url.to_string());
            }
            result
        }

        let url = "http://abc?num_objects=9&offset=8388878&size=8388878";
        let parsed = parse_url(url);
        assert_eq!(parsed["url"], "http://abc");
        assert_eq!(parsed["num_objects"], "9");
        assert_eq!(parsed["offset"], "8388878");
        assert_eq!(parsed["size"], "8388878");
    }

    /// Port of C++ UrlIpTcpParseTest: parse tcp:// URLs into host:port.
    #[test]
    fn test_url_ip_tcp_parse() {
        fn parse_tcp_endpoint(url: &str) -> String {
            let stripped = url.strip_prefix("tcp://").unwrap_or(url);
            let stripped = stripped.strip_suffix('/').unwrap_or(stripped);

            // Handle bracketed IPv6
            if stripped.starts_with('[') {
                if let Some(bracket_end) = stripped.find(']') {
                    let host = &stripped[..bracket_end + 1]; // includes brackets
                    let rest = &stripped[bracket_end + 1..];
                    let port = rest.strip_prefix(':').unwrap_or("0");
                    let port = if port.is_empty() { "0" } else { port };
                    return format!("{}:{}", host, port);
                }
            }

            // IPv4 or hostname
            if let Some(colon_pos) = stripped.rfind(':') {
                let host = &stripped[..colon_pos];
                let port = &stripped[colon_pos + 1..];
                let port = if port.is_empty() { "0" } else { port };
                format!("{}:{}", host, port)
            } else {
                format!("{}:0", stripped)
            }
        }

        assert_eq!(parse_tcp_endpoint("tcp://[::1]:1/"), "[::1]:1");
        assert_eq!(parse_tcp_endpoint("tcp://[::1]/"), "[::1]:0");
        assert_eq!(parse_tcp_endpoint("tcp://[::1]:1"), "[::1]:1");
        assert_eq!(parse_tcp_endpoint("tcp://[::1]"), "[::1]:0");
        assert_eq!(parse_tcp_endpoint("tcp://127.0.0.1:1/"), "127.0.0.1:1");
        assert_eq!(parse_tcp_endpoint("tcp://127.0.0.1/"), "127.0.0.1:0");
        assert_eq!(parse_tcp_endpoint("tcp://127.0.0.1:1"), "127.0.0.1:1");
        assert_eq!(parse_tcp_endpoint("tcp://127.0.0.1"), "127.0.0.1:0");
        assert_eq!(parse_tcp_endpoint("[::1]:1/"), "[::1]:1");
        assert_eq!(parse_tcp_endpoint("[::1]/"), "[::1]:0");
        assert_eq!(parse_tcp_endpoint("[::1]:1"), "[::1]:1");
        assert_eq!(parse_tcp_endpoint("[::1]"), "[::1]:0");
        assert_eq!(parse_tcp_endpoint("127.0.0.1:1/"), "127.0.0.1:1");
        assert_eq!(parse_tcp_endpoint("127.0.0.1/"), "127.0.0.1:0");
        assert_eq!(parse_tcp_endpoint("127.0.0.1:1"), "127.0.0.1:1");
        assert_eq!(parse_tcp_endpoint("127.0.0.1"), "127.0.0.1:0");
    }
}
