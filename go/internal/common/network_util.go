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

package common

import (
	"net"
	"os"
	"sync"

	"github.com/ray-project/ray/go/pkg/log"
)

var (
	localhostIP     string
	localhostIPOnce sync.Once
)

type testAddress struct {
	addr     string
	network  string
	protocol string
}

// GetNodeIpAddress returns the IP address of this node.
//
// This function implements the same IP detection algorithm as C++
// (src/ray/util/network_util.cc:GetNodeIpAddressFromPerspective)
// and Python (python/ray/includes/network_util.pxi). All three languages
// share the following core logic:
// 1. Prefer the provided address parameter for a socket-connectivity check.
// 2. Fall back to an external connectivity check (UDP to a public DNS server).
// 3. Fall back again to hostname resolution.
// 4. Finally, default to 127.0.0.1.
//
// Reference implementations:
// - C++: src/ray/util/network_util.cc:257-327
// - Python: python/ray/includes/network_util.pxi:1-9
func GetNodeIpAddress(address *string) string {

	// In non-cluster mode, return the localhost IP directly.
	if !EnableRayCluster() {
		nodeIp := GetLocalhostIP()
		log.Log.V(1).Info("Using localhost IP (non-cluster mode)", "ip", nodeIp)
		return nodeIp
	}

	// In cluster mode, detect the node IP address.
	nodeIp := GetNodeIpAddressFromPerspective(address)
	log.Log.V(1).Info("Detected node IP address", "ip", nodeIp, "perspective", address)
	return nodeIp
}

func GetLocalhostIP() string {
	localhostIPOnce.Do(func() {
		for _, network := range []string{"udp4", "udp6"} {
			conn, err := net.Dial(network, "localhost:0")
			if err == nil {
				defer conn.Close()
				localAddr := conn.LocalAddr().(*net.UDPAddr)
				if localAddr.IP != nil {
					localhostIP = localAddr.IP.String()
					log.Log.V(1).Info("Detected localhost IP", "ip", localhostIP, "network", network)
					return
				}
			}
		}

		localhostIP = "127.0.0.1"
		log.Log.V(1).Info("Using default localhost IP", "ip", localhostIP)
	})

	return localhostIP
}

func GetNodeIpAddressFromPerspective(address *string) string {
	if address != nil && *address != "" {
		if ip := tryDetectIPFromAddress(*address); ip != "" {
			return ip
		}
	}

	if ip := tryDetectIPFromExternalConnectivity(); ip != "" {
		return ip
	}

	if ip := tryDetectIPFromHostname(); ip != "" {
		return ip
	}

	log.Log.V(0).Info("Unable to detect local IP address, defaulting to 127.0.0.1")
	return "127.0.0.1"
}

func tryDetectIPFromAddress(addr string) string {
	// The perspective address is typically "host:port" (e.g. the GCS address),
	// so resolve the host portion only. Mirrors the C++ implementation, which
	// splits host and port before dialing the endpoint.
	host := addr
	if h, _, err := net.SplitHostPort(addr); err == nil {
		host = h
	}

	ip := net.ParseIP(host)
	if ip == nil {
		ips, err := net.LookupIP(host)
		if err != nil || len(ips) == 0 {
			log.Log.V(0).Error(err, "Failed to resolve hostname", "host", host)
			return ""
		}
		if ips[0] == nil {
			log.Log.V(0).Info("First resolved IP is nil", "host", host)
			return ""
		}
		ip = ips[0]
	}

	var testAddr testAddress
	if ip.To4() != nil {
		testAddr = testAddress{addr, "udp4", "IPv4"}
	} else {
		testAddr = testAddress{addr, "udp6", "IPv6"}
	}

	conn, err := net.Dial(testAddr.network, testAddr.addr)
	if err == nil {
		defer conn.Close()
		localAddr := conn.LocalAddr().(*net.UDPAddr)
		if localAddr.IP != nil {
			ip := localAddr.IP.String()
			log.Log.V(0).Info("Detected IP via external connectivity", "ip", ip, "target", testAddr.addr, "protocol", testAddr.protocol)
			return ip
		}
	}

	return ""
}

func tryDetectIPFromExternalConnectivity() string {
	testAddresses := getDefaultTestAddresses()
	for _, testAddr := range testAddresses {
		conn, err := net.Dial(testAddr.network, testAddr.addr)
		if err == nil {
			defer conn.Close()
			localAddr := conn.LocalAddr().(*net.UDPAddr)
			if localAddr.IP != nil {
				ip := localAddr.IP.String()
				log.Log.V(0).Info("Detected IP via external connectivity", "ip", ip, "target", testAddr.addr, "protocol", testAddr.protocol)
				return ip
			}
		}
	}

	return ""
}

func tryDetectIPFromHostname() string {
	hostname, err := os.Hostname()
	if err != nil {
		log.Log.V(0).Info("Failed to get hostname", "error", err)
		return ""
	}

	addrs, err := net.LookupIP(hostname)
	if err != nil {
		log.Log.V(0).Info("Failed to lookup IP for hostname", "hostname", hostname, "error", err)
		return ""
	}

	for _, addr := range addrs {
		if addr.To4() != nil {
			log.Log.V(0).Info("Found IPv4 address from hostname", "ip", addr.String(), "hostname", hostname)
			return addr.String()
		}
	}

	for _, addr := range addrs {
		if addr.To16() != nil {
			log.Log.V(0).Info("Found IPv6 address from hostname", "ip", addr.String(), "hostname", hostname)
			return addr.String()
		}
	}

	return ""
}

func getDefaultTestAddresses() []testAddress {
	return []testAddress{
		{"8.8.8.8:53", "udp4", "IPv4"},
		{"[2001:4860:4860::8888]:53", "udp6", "IPv6"},
	}
}

func IsLocalhost(host string) bool {
	return host == "localhost" || host == "127.0.0.1" || host == "::1"
}

func ReplaceLocalhostWithNodeIP(host string, nodeIP string) string {
	if IsLocalhost(host) {
		return nodeIP
	}
	return host
}
