// Copyright 2026 The Ray Authors.
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

package subscribers

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"golang.org/x/crypto/ssh"

	"github.com/ray-project/ray/go/internal/autoscaler/v2/instance_manager"
	"github.com/ray-project/ray/go/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newTestInstallerConfig builds a config with auth and
// worker_start_ray_commands.
func newTestInstallerConfig(auth map[string]interface{}, commands interface{}) *instance_manager.AutoscalingConfig {
	configs := map[string]interface{}{}
	if auth != nil {
		configs["auth"] = auth
	}
	if commands != nil {
		configs["worker_start_ray_commands"] = commands
	}
	return &instance_manager.AutoscalingConfig{Configs: configs}
}

// writeTestSSHKey generates an RSA private key, writes it to a temp file and
// returns the file path.
func writeTestSSHKey(t *testing.T) string {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	keyPath := filepath.Join(t.TempDir(), "id_rsa_test")
	pemBytes := pem.EncodeToMemory(&pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(key),
	})
	require.NoError(t, os.WriteFile(keyPath, pemBytes, 0o600))
	return keyPath
}

// TestNewSSHRayInstaller_Config parses auth and worker_start_ray_commands,
// falling back to the defaults when unconfigured
// (ssh_user=ubuntu, local default start commands).
func TestNewSSHRayInstaller_Config(t *testing.T) {
	// Default config.
	installer := NewSSHRayInstaller(newTestInstallerConfig(nil, nil))
	assert.Equal(t, "ubuntu", installer.sshUser)
	assert.Equal(t, "", installer.sshPrivateKeyPath)
	assert.Equal(t, defaultWorkerStartRayCommands, installer.workerStartRayCommands)

	// Custom config (a list parsed from YAML is []interface{}).
	installer = NewSSHRayInstaller(newTestInstallerConfig(
		map[string]interface{}{
			"ssh_user":        "root",
			"ssh_private_key": "/tmp/key.pem",
		},
		[]interface{}{"ray stop", "ray start --address=$RAY_HEAD_IP:6379 --block"},
	))
	assert.Equal(t, "root", installer.sshUser)
	assert.Equal(t, "/tmp/key.pem", installer.sshPrivateKeyPath)
	assert.Equal(t,
		[]string{"ray stop", "ray start --address=$RAY_HEAD_IP:6379 --block"},
		installer.workerStartRayCommands)
}

// TestBuildInstallCommands injects the env vars into the prefix of each
// command (RAY_HEAD_IP etc.).
func TestBuildInstallCommands(t *testing.T) {
	cloudId := "127.0.0.2"
	instance := &proto.Instance{
		InstanceId:      "i-1",
		InstanceType:    "local.cluster.node",
		CloudInstanceId: &cloudId,
	}

	commands := buildInstallCommands(instance, "127.0.0.1",
		[]string{"ray stop", "ray start --address=$RAY_HEAD_IP:6379"})

	assert.Equal(t, []string{
		"RAY_HEAD_IP=127.0.0.1 RAY_CLOUD_INSTANCE_ID=127.0.0.2 RAY_NODE_TYPE_NAME=local.cluster.node ray stop",
		"RAY_HEAD_IP=127.0.0.1 RAY_CLOUD_INSTANCE_ID=127.0.0.2 RAY_NODE_TYPE_NAME=local.cluster.node ray start --address=$RAY_HEAD_IP:6379",
	}, commands)

	// An empty command list falls back to the default start commands.
	assert.Equal(t, 2, len(buildInstallCommands(instance, "127.0.0.1", nil)))
}

// TestSSHRayInstaller_MissingCloudInstanceId errors when there is no cloud
// instance ID.
func TestSSHRayInstaller_MissingCloudInstanceId(t *testing.T) {
	installer := NewSSHRayInstaller(newTestInstallerConfig(nil, nil))

	err := installer.InstallRay(&proto.Instance{InstanceId: "i-1"}, "127.0.0.1")

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no cloud instance id")
}

// TestSSHRayInstaller_NoAuthConfigured errors when no private key is
// configured and there is no ssh agent.
func TestSSHRayInstaller_NoAuthConfigured(t *testing.T) {
	t.Setenv("SSH_AUTH_SOCK", "")
	installer := NewSSHRayInstaller(newTestInstallerConfig(nil, nil))
	cloudId := "127.0.0.2"

	err := installer.InstallRay(
		&proto.Instance{InstanceId: "i-1", CloudInstanceId: &cloudId}, "127.0.0.1")

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no ssh auth method")
}

// TestSSHRayInstaller_DialFailure errors when the ssh connection fails.
func TestSSHRayInstaller_DialFailure(t *testing.T) {
	keyPath := writeTestSSHKey(t)
	installer := NewSSHRayInstaller(newTestInstallerConfig(
		map[string]interface{}{"ssh_user": "test", "ssh_private_key": keyPath}, nil))
	installer.sshPort = "1" // nothing listens on local port 1, the connection is refused immediately
	cloudId := "127.0.0.1"

	err := installer.InstallRay(
		&proto.Instance{InstanceId: "i-1", CloudInstanceId: &cloudId}, "127.0.0.1")

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to ssh into node")
}

// ---------- Embedded ssh server: verifying the commands really run ----------

// fakeSSHServer is an embedded ssh service recording the commands executed by
// the client.
type fakeSSHServer struct {
	listener net.Listener
	mu       sync.Mutex
	commands []string
}

// newFakeSSHServer starts the embedded ssh service on a random local port.
func newFakeSSHServer(t *testing.T) *fakeSSHServer {
	t.Helper()
	hostKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	hostSigner, err := ssh.NewSignerFromKey(hostKey)
	require.NoError(t, err)

	config := &ssh.ServerConfig{
		// Accept any client public key (test environment).
		PublicKeyCallback: func(conn ssh.ConnMetadata, key ssh.PublicKey) (*ssh.Permissions, error) {
			return &ssh.Permissions{}, nil
		},
	}
	config.AddHostKey(hostSigner)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	server := &fakeSSHServer{listener: listener}
	go server.serve(config)
	t.Cleanup(func() { listener.Close() })
	return server
}

func (s *fakeSSHServer) serve(config *ssh.ServerConfig) {
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			return
		}
		go s.handleConn(conn, config)
	}
}

func (s *fakeSSHServer) handleConn(conn net.Conn, config *ssh.ServerConfig) {
	serverConn, channels, requests, err := ssh.NewServerConn(conn, config)
	if err != nil {
		conn.Close()
		return
	}
	defer serverConn.Close()
	go ssh.DiscardRequests(requests)
	for newChannel := range channels {
		if newChannel.ChannelType() != "session" {
			newChannel.Reject(ssh.UnknownChannelType, "unsupported channel type")
			continue
		}
		channel, requests, err := newChannel.Accept()
		if err != nil {
			continue
		}
		go s.handleSession(channel, requests)
	}
}

func (s *fakeSSHServer) handleSession(channel ssh.Channel, requests <-chan *ssh.Request) {
	defer channel.Close()
	for request := range requests {
		if request.Type != "exec" {
			if request.WantReply {
				request.Reply(false, nil)
			}
			continue
		}
		var payload struct {
			Command string
		}
		if err := ssh.Unmarshal(request.Payload, &payload); err != nil {
			continue
		}
		s.mu.Lock()
		s.commands = append(s.commands, payload.Command)
		s.mu.Unlock()

		channel.Write([]byte("done\n"))
		if request.WantReply {
			request.Reply(true, nil)
		}
		// exit status 0 -> the client's CombinedOutput returns nil.
		channel.SendRequest("exit-status", false,
			ssh.Marshal(struct{ Status uint32 }{Status: 0}))
		return
	}
}

func (s *fakeSSHServer) recordedCommands() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.commands...)
}

// TestSSHRayInstaller_RunsCommandsOnServer runs the env-var-prefixed start
// commands in order over a real ssh session.
func TestSSHRayInstaller_RunsCommandsOnServer(t *testing.T) {
	server := newFakeSSHServer(t)
	keyPath := writeTestSSHKey(t)
	installer := NewSSHRayInstaller(newTestInstallerConfig(
		map[string]interface{}{"ssh_user": "test", "ssh_private_key": keyPath},
		[]interface{}{"ray stop", "ray start --address=$RAY_HEAD_IP:6379"},
	))
	// Connect to the embedded ssh server instead of the default port 22.
	_, port, err := net.SplitHostPort(server.listener.Addr().String())
	require.NoError(t, err)
	installer.sshPort = port

	cloudId := "127.0.0.1"
	err = installer.InstallRay(&proto.Instance{
		InstanceId:      "i-1",
		InstanceType:    "local.cluster.node",
		CloudInstanceId: &cloudId,
	}, "10.0.0.1")

	assert.NoError(t, err)
	assert.Equal(t, []string{
		"RAY_HEAD_IP=10.0.0.1 RAY_CLOUD_INSTANCE_ID=127.0.0.1 RAY_NODE_TYPE_NAME=local.cluster.node ray stop",
		"RAY_HEAD_IP=10.0.0.1 RAY_CLOUD_INSTANCE_ID=127.0.0.1 RAY_NODE_TYPE_NAME=local.cluster.node ray start --address=$RAY_HEAD_IP:6379",
	}, server.recordedCommands())
}

// TestToStringSlice parses both the YAML and the JSON list shapes.
func TestToStringSlice(t *testing.T) {
	assert.Equal(t, []string{"a", "b"}, toStringSlice([]interface{}{"a", "b"}))
	assert.Equal(t, []string{"a"}, toStringSlice([]string{"a"}))
	assert.Nil(t, toStringSlice(nil))
	assert.Nil(t, toStringSlice("not a list"))
	assert.Equal(t, []string{"a"}, toStringSlice([]interface{}{"a", 42}))
}
