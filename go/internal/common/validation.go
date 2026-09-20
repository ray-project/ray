// Copyright 2025 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package common provides common utility functions for validation and other operations.
package common

import (
	"fmt"
	"net"
	"strconv"
	"strings"
)

// Port validation constants
const (
	MinPort = 1
	MaxPort = 65535
)

// ValidateIPAddress validates an IP address string.
// Returns true if the IP address is valid (supports both IPv4 and IPv6).
func ValidateIPAddress(ip string) bool {
	if ip == "" {
		return false
	}
	return net.ParseIP(ip) != nil
}

// ValidatePort validates a port number.
// Returns true if the port is in the valid range (1-65535).
func ValidatePort(port int) bool {
	return port >= MinPort && port <= MaxPort
}

// ValidatePortAllowZero validates a port number, allowing 0 for random port.
// Returns true if the port is 0 (random) or in the valid range (1-65535).
// Use this when port 0 is acceptable (e.g., random port assignment).
func ValidatePortAllowZero(port int) bool {
	return port == 0 || (port >= MinPort && port <= MaxPort)
}

// ValidateHostPort validates a host:port address string, allowing port 0.
// Returns the host, port, and error.
// Port 0 is allowed (represents random port assignment).
func ValidateHostPort(address string) (host string, port int, err error) {
	if address == "" {
		return "", 0, fmt.Errorf("address cannot be empty")
	}

	host, portStr, err := net.SplitHostPort(address)
	if err != nil {
		return "", 0, fmt.Errorf("invalid address format: %s (expected host:port)", address)
	}

	if host == "" {
		return "", 0, fmt.Errorf("address missing host: %s", address)
	}

	port, err = strconv.Atoi(portStr)
	if err != nil {
		return "", 0, fmt.Errorf("invalid address port: %s", portStr)
	}

	if !ValidatePortAllowZero(port) {
		return "", 0, fmt.Errorf("invalid port: %d (must be 0 or %d-%d)", port, MinPort, MaxPort)
	}

	return host, port, nil
}

// ValidateSocketPath validates a socket path.
// Returns true if the socket path is valid (non-empty and reasonable length).
// A valid socket path must:
// 1. Be non-empty
// 2. Not exceed maximum path length (108 characters for Unix domain sockets)
func ValidateSocketPath(path string) bool {
	if path == "" {
		return false
	}
	// Unix domain socket paths are limited to 108 characters (including null terminator)
	// We use 107 to be safe
	if len(path) > 107 {
		return false
	}
	return true
}

// BuildAddress builds a network address string, supporting both IPv4 and IPv6.
//
// Format rules:
// - For IPv6 addresses (host containing a colon), returns "[host]:port".
// - For IPv4 addresses or hostnames, returns "host:port".
func BuildAddress(host string, port int) (string, error) {
	if !ValidatePortAllowZero(port) {
		return "", fmt.Errorf("invalid port: %d (must be 0 or %d-%d)", port, MinPort, MaxPort)
	}
	// Detect an IPv6 address by checking for a colon.
	if strings.Contains(host, ":") {
		// IPv6 addresses must be wrapped in square brackets.
		return fmt.Sprintf("[%s]:%d", host, port), nil
	}
	// IPv4 addresses or hostnames use the plain "host:port" format.
	return fmt.Sprintf("%s:%d", host, port), nil
}
