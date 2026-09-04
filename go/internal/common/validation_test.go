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
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestValidateIPAddress(t *testing.T) {
	tests := []struct {
		name     string
		ip       string
		expected bool
	}{
		// Valid IPv4 addresses
		{"IPv4 localhost", "127.0.0.1", true},
		{"IPv4 zero", "0.0.0.0", true},
		{"IPv4 standard", "192.168.1.1", true},
		{"IPv4 public", "8.8.8.8", true},

		// Valid IPv6 addresses
		{"IPv6 localhost", "::1", true},
		{"IPv6 all zeros", "::", true},
		{"IPv6 full", "2001:0db8:85a3:0000:0000:8a2e:0370:7334", true},
		{"IPv6 compressed", "2001:db8::1", true},

		// Invalid addresses
		{"empty string", "", false},
		{"invalid format", "256.256.256.256", false},
		{"invalid IPv4", "192.168.1", false},
		{"invalid chars", "192.168.1.1.1", false},
		{"text only", "not-an-ip", false},
		{"partial IPv6", "2001:db8", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ValidateIPAddress(tt.ip)
			if result != tt.expected {
				t.Errorf("ValidateIPAddress(%q) = %v, expected %v", tt.ip, result, tt.expected)
			}
		})
	}
}

func TestValidatePort(t *testing.T) {
	tests := []struct {
		name     string
		port     int
		expected bool
	}{
		{"valid min", MinPort, true},
		{"valid max", MaxPort, true},
		{"valid middle", 8080, true},
		{"valid http", 80, true},
		{"valid https", 443, true},

		{"zero", 0, false},
		{"negative", -1, false},
		{"too large", MaxPort + 1, false},
		{"way too large", 100000, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ValidatePort(tt.port)
			if result != tt.expected {
				t.Errorf("ValidatePort(%d) = %v, expected %v", tt.port, result, tt.expected)
			}
		})
	}
}

func TestValidatePortAllowZero(t *testing.T) {
	tests := []struct {
		name     string
		port     int
		expected bool
	}{
		{"zero (random)", 0, true},
		{"valid min", MinPort, true},
		{"valid max", MaxPort, true},
		{"valid middle", 8080, true},

		{"negative", -1, false},
		{"too large", MaxPort + 1, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ValidatePortAllowZero(tt.port)
			if result != tt.expected {
				t.Errorf("ValidatePortAllowZero(%d) = %v, expected %v", tt.port, result, tt.expected)
			}
		})
	}
}

func TestValidateHostPort(t *testing.T) {
	tests := []struct {
		name        string
		address     string
		expectHost  string
		expectPort  int
		expectError bool
	}{
		// Valid addresses
		{"IPv4 with port", "127.0.0.1:8080", "127.0.0.1", 8080, false},
		{"IPv4 with zero port", "127.0.0.1:0", "127.0.0.1", 0, false},
		{"IPv4 with min port", "192.168.1.1:1", "192.168.1.1", 1, false},
		{"IPv4 with max port", "10.0.0.1:65535", "10.0.0.1", 65535, false},
		{"IPv6 with port", "[::1]:8080", "::1", 8080, false},
		{"localhost with port", "localhost:3000", "localhost", 3000, false},
		{"hostname with port", "example.com:443", "example.com", 443, false},

		// Invalid addresses
		{"empty string", "", "", 0, true},
		{"missing port", "127.0.0.1", "", 0, true},
		{"missing host", ":0", "", 0, true},
		{"invalid port (negative)", "127.0.0.1:-1", "", 0, true},
		{"invalid port (too large)", "127.0.0.1:70000", "", 0, true},
		{"invalid port (non-numeric)", "127.0.0.1:abc", "", 0, true},
		{"malformed", "127.0.0.1:8080:extra", "", 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			host, port, err := ValidateHostPort(tt.address)
			if tt.expectError {
				if err == nil {
					t.Errorf("ValidateHostPort(%q) expected error, got nil", tt.address)
				}
			} else {
				if err != nil {
					t.Errorf("ValidateHostPort(%q) unexpected error: %v", tt.address, err)
				}
				if host != tt.expectHost {
					t.Errorf("ValidateHostPort(%q) host = %q, expected %q", tt.address, host, tt.expectHost)
				}
				if port != tt.expectPort {
					t.Errorf("ValidateHostPort(%q) port = %d, expected %d", tt.address, port, tt.expectPort)
				}
			}
		})
	}
}

func TestValidateSocketPath(t *testing.T) {
	tests := []struct {
		name     string
		path     string
		expected bool
	}{
		{"valid unix socket", "/tmp/ray/socket", true},
		{"valid windows socket", `\\.\pipe\ray`, true},
		{"valid short path", "/a/b", true},
		{"empty string", "", false},
		{"whitespace only", "   ", true},                              // Whitespace-only string (passes non-empty check)
		{"path too long", "/tmp/" + string(make([]byte, 110)), false}, // Path exceeds 107 characters
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ValidateSocketPath(tt.path)
			if result != tt.expected {
				t.Errorf("ValidateSocketPath(%q) = %v, expected %v", tt.path, result, tt.expected)
			}
		})
	}
}

func TestBuildAddress(t *testing.T) {
	tests := []struct {
		name        string
		host        string
		port        int
		wantAddr    string
		wantErr     bool
		description string
	}{
		{"IPv4 localhost", "127.0.0.1", 6379, "127.0.0.1:6379", false, "IPv4 localhost should return host:port"},
		{"IPv4 zero", "0.0.0.0", 8080, "0.0.0.0:8080", false, "IPv4 zero address should return host:port"},
		{"IPv4 standard", "192.168.1.1", 80, "192.168.1.1:80", false, "IPv4 standard address should return host:port"},
		{"IPv4 public", "8.8.8.8", 443, "8.8.8.8:443", false, "IPv4 public address should return host:port"},

		{"IPv6 localhost", "::1", 6379, "[::1]:6379", false, "IPv6 localhost should return [host]:port"},
		{"IPv6 all zeros", "::", 8080, "[::]:8080", false, "IPv6 all zeros should return [host]:port"},
		{"IPv6 full", "2001:0db8:85a3:0000:0000:8a2e:0370:7334", 80, "[2001:0db8:85a3:0000:0000:8a2e:0370:7334]:80", false, "Full IPv6 should return [host]:port"},
		{"IPv6 compressed", "2001:db8::1", 443, "[2001:db8::1]:443", false, "Compressed IPv6 should return [host]:port"},
		{"IPv6 link-local", "fe80::1", 8080, "[fe80::1]:8080", false, "Link-local IPv6 should return [host]:port"},

		{"hostname localhost", "localhost", 6379, "localhost:6379", false, "Hostname localhost should return host:port"},
		{"hostname with domain", "example.com", 443, "example.com:443", false, "Hostname with domain should return host:port"},
		{"hostname subdomain", "api.example.com", 8080, "api.example.com:8080", false, "Subdomain hostname should return host:port"},

		{"port zero", "127.0.0.1", 0, "127.0.0.1:0", false, "Port zero should be valid"},
		{"port min", "127.0.0.1", 1, "127.0.0.1:1", false, "Port 1 should be valid"},
		{"port max", "127.0.0.1", 65535, "127.0.0.1:65535", false, "Port 65535 should be valid"},
		{"port http", "127.0.0.1", 80, "127.0.0.1:80", false, "HTTP port 80 should be valid"},
		{"port https", "127.0.0.1", 443, "127.0.0.1:443", false, "HTTPS port 443 should be valid"},

		{"IPv6 mapped IPv4", "::ffff:192.0.2.1", 80, "[::ffff:192.0.2.1]:80", false, "IPv6-mapped IPv4 should return [host]:port"},
		{"IPv6 loopback", "::1", 6379, "[::1]:6379", false, "IPv6 loopback should return [host]:port"},

		// Negative ports are not validated by BuildAddress but must still be handled.
		{"negative port", "127.0.0.1", -1, "127.0.0.1:-1", true, "Negative port should still format correctly"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotAddr, err := BuildAddress(tt.host, tt.port)

			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.wantAddr, gotAddr, tt.description)
			}
		})
	}
}

func TestBuildAddress_EdgeCases(t *testing.T) {
	t.Run("empty host with port", func(t *testing.T) {
		addr, err := BuildAddress("", 6379)
		assert.NoError(t, err)
		assert.Equal(t, ":6379", addr, "Empty host should return :port")
	})

	t.Run("host with multiple colons (IPv6)", func(t *testing.T) {
		addr, err := BuildAddress("2001:db8:85a3::8a2e:370:7334", 8080)
		assert.NoError(t, err)
		assert.Equal(t, "[2001:db8:85a3::8a2e:370:7334]:8080", addr, "Complex IPv6 should be properly bracketed")
	})

	t.Run("very large port number", func(t *testing.T) {
		_, err := BuildAddress("127.0.0.1", 999999)
		assert.Error(t, err)
	})

	t.Run("IPv6 with zone ID", func(t *testing.T) {
		// Link-local addresses often have zone IDs like fe80::1%eth0
		addr, err := BuildAddress("fe80::1%eth0", 8080)
		assert.NoError(t, err)
		assert.Equal(t, "[fe80::1%eth0]:8080", addr, "IPv6 with zone ID should be bracketed")
	})

	t.Run("hostname with hyphens", func(t *testing.T) {
		addr, err := BuildAddress("my-host-name.example.com", 443)
		assert.NoError(t, err)
		assert.Equal(t, "my-host-name.example.com:443", addr, "Hostname with hyphens should work")
	})

	t.Run("hostname with numbers", func(t *testing.T) {
		addr, err := BuildAddress("host123.example456.com", 8080)
		assert.NoError(t, err)
		assert.Equal(t, "host123.example456.com:8080", addr, "Hostname with numbers should work")
	})

	t.Run("single character hostname", func(t *testing.T) {
		addr, err := BuildAddress("a", 80)
		assert.NoError(t, err)
		assert.Equal(t, "a:80", addr, "Single character hostname should work")
	})

	t.Run("long hostname", func(t *testing.T) {
		longHost := "very-long-hostname-that-is-still-valid-according-to-dns-specifications.example.com"
		addr, err := BuildAddress(longHost, 443)
		assert.NoError(t, err)
		assert.Equal(t, longHost+":443", addr, "Long hostname should work")
	})

	t.Run("IPv4 in IPv6 format", func(t *testing.T) {
		addr, err := BuildAddress("::ffff:127.0.0.1", 8080)
		assert.NoError(t, err)
		assert.Equal(t, "[::ffff:127.0.0.1]:8080", addr, "IPv4-mapped IPv6 should be bracketed")
	})

	t.Run("port as string boundary", func(t *testing.T) {
		// Test around common port boundaries
		tests := []struct {
			port     int
			expected string
		}{
			{21, "127.0.0.1:21"},       // FTP
			{22, "127.0.0.1:22"},       // SSH
			{23, "127.0.0.1:23"},       // Telnet
			{25, "127.0.0.1:25"},       // SMTP
			{53, "127.0.0.1:53"},       // DNS
			{110, "127.0.0.1:110"},     // POP3
			{143, "127.0.0.1:143"},     // IMAP
			{993, "127.0.0.1:993"},     // IMAPS
			{995, "127.0.0.1:995"},     // POP3S
			{3306, "127.0.0.1:3306"},   // MySQL
			{5432, "127.0.0.1:5432"},   // PostgreSQL
			{6379, "127.0.0.1:6379"},   // Redis
			{27017, "127.0.0.1:27017"}, // MongoDB
		}

		for _, test := range tests {
			addr, err := BuildAddress("127.0.0.1", test.port)
			assert.NoError(t, err)
			assert.Equal(t, test.expected, addr, "Port %d should format correctly", test.port)
		}
	})
}

func TestBuildAddress_IPv4VsIPv6Detection(t *testing.T) {
	t.Run("simple IPv4 detection", func(t *testing.T) {
		// IPv4 addresses don't contain colons
		ipv4Addresses := []string{
			"127.0.0.1",
			"192.168.1.1",
			"10.0.0.1",
			"172.16.0.1",
			"8.8.8.8",
			"1.1.1.1",
		}

		for _, ip := range ipv4Addresses {
			addr, err := BuildAddress(ip, 8080)
			assert.NoError(t, err)
			assert.NotContains(t, addr, "[", "IPv4 address %s should not have brackets", ip)
			assert.NotContains(t, addr, "]", "IPv4 address %s should not have brackets", ip)
		}
	})

	t.Run("IPv6 detection with various formats", func(t *testing.T) {
		// All IPv6 addresses contain colons and should be bracketed
		ipv6Addresses := []struct {
			address  string
			expected string
		}{
			{"::1", "[::1]:8080"},
			{"::", "[::]:8080"},
			{"2001:db8::1", "[2001:db8::1]:8080"},
			{"fe80::1", "[fe80::1]:8080"},
			{"ff02::1", "[ff02::1]:8080"},
			{"2001:0db8:0000:0000:0000:0000:0000:0001", "[2001:0db8:0000:0000:0000:0000:0000:0001]:8080"},
		}

		for _, test := range ipv6Addresses {
			addr, err := BuildAddress(test.address, 8080)
			assert.NoError(t, err)
			assert.Equal(t, test.expected, addr, "IPv6 address %s should be bracketed", test.address)
			assert.Contains(t, addr, "[", "IPv6 address should start with bracket")
			assert.Contains(t, addr, "]", "IPv6 address should end with bracket before port")
		}
	})

	t.Run("hostname vs IP distinction", func(t *testing.T) {
		// Hostnames typically don't contain colons
		hostnames := []string{
			"localhost",
			"example.com",
			"api.service.local",
			"my-server-01.prod.example.com",
		}

		for _, hostname := range hostnames {
			addr, err := BuildAddress(hostname, 443)
			assert.NoError(t, err)
			assert.NotContains(t, addr, "[", "Hostname %s should not have brackets", hostname)
			assert.NotContains(t, addr, "]", "Hostname %s should not have brackets", hostname)
		}
	})
}

func TestBuildAddress_PerformanceAndStress(t *testing.T) {
	t.Run("rapid calls with different inputs", func(t *testing.T) {
		// Test that the function can handle rapid calls without issues
		inputs := []struct {
			host string
			port int
		}{
			{"127.0.0.1", 6379},
			{"::1", 6379},
			{"localhost", 6379},
			{"192.168.1.1", 8080},
			{"2001:db8::1", 8080},
			{"example.com", 443},
		}

		for i := 0; i < 1000; i++ {
			for _, input := range inputs {
				_, err := BuildAddress(input.host, input.port)
				assert.NoError(t, err, "Rapid call %d with host=%s, port=%d should succeed", i, input.host, input.port)
			}
		}
	})

	t.Run("concurrent calls (if function is safe for concurrent use)", func(t *testing.T) {
		// BuildAddress should be safe for concurrent use since it doesn't maintain state
		done := make(chan bool, 10)

		for i := 0; i < 10; i++ {
			go func() {
				defer func() { done <- true }()
				_, err := BuildAddress("127.0.0.1", 8080)
				assert.NoError(t, err)
			}()
		}

		// Wait for all goroutines to complete
		for i := 0; i < 10; i++ {
			<-done
		}
	})
}

func TestBuildAddress_RealWorldScenarios(t *testing.T) {
	t.Run("Ray GCS address format", func(t *testing.T) {
		// Ray GCS typically uses addresses like 127.0.0.1:6379
		addr, err := BuildAddress("127.0.0.1", 6379)
		assert.NoError(t, err)
		assert.Equal(t, "127.0.0.1:6379", addr, "Ray GCS address should be in host:port format")
	})

	t.Run("Kubernetes service address", func(t *testing.T) {
		// Kubernetes services are often accessed via DNS names
		addr, err := BuildAddress("ray-head-svc.default.svc.cluster.local", 6379)
		assert.NoError(t, err)
		assert.Equal(t, "ray-head-svc.default.svc.cluster.local:6379", addr, "K8s service address should work")
	})

	t.Run("Docker container networking", func(t *testing.T) {
		// Docker containers might use host.docker.internal
		addr, err := BuildAddress("host.docker.internal", 6379)
		assert.NoError(t, err)
		assert.Equal(t, "host.docker.internal:6379", addr, "Docker internal hostname should work")
	})

	t.Run("Cloud provider internal DNS", func(t *testing.T) {
		// AWS EC2 internal DNS
		addr, err := BuildAddress("ip-10-0-1-100.ec2.internal", 6379)
		assert.NoError(t, err)
		assert.Equal(t, "ip-10-0-1-100.ec2.internal:6379", addr, "AWS internal DNS should work")
	})

	t.Run("Load balancer address", func(t *testing.T) {
		// Load balancers often use DNS names
		addr, err := BuildAddress("my-load-balancer-123456789.us-west-2.elb.amazonaws.com", 443)
		assert.NoError(t, err)
		assert.Equal(t, "my-load-balancer-123456789.us-west-2.elb.amazonaws.com:443", addr, "ELB address should work")
	})
}
