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
	"fmt"
	"net"
	"os"
	"time"

	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/agent"

	"github.com/ray-project/ray/go/internal/autoscaler"
	"github.com/ray-project/ray/go/internal/autoscaler/v2/instance_manager"
	"github.com/ray-project/ray/go/pkg/log"
	"github.com/ray-project/ray/go/proto"
)

// Defaults for the simplified ssh installer.
const (
	defaultSSHUser        = "ubuntu" // ray-schema.json auth.ssh_user default
	defaultSSHDialTimeout = 10 * time.Second
	// defaultSSHCommandTimeout must cover `ray stop` + `ray start`; the
	// default start command returns once the raylet daemonizes (no --block).
	defaultSSHCommandTimeout = 2 * time.Minute
)

// defaultWorkerStartRayCommands mirrors the local provider defaults
// (go/internal/autoscaler/local/defaults.yaml worker_start_ray_commands).
var defaultWorkerStartRayCommands = []string{
	"ray stop",
	"ray start --address=$RAY_HEAD_IP:6379",
}

// SSHRayInstaller is the simplified Ray installer (Phase 2):
// it ssh's into the target instance and runs the worker start ray commands
// only, skipping file syncing, docker and setup commands - nodes are
// expected to have Ray pre-installed. It corresponds to the Python
// RayInstaller + NodeUpdater + SSHCommandRunner chain reduced to plain ssh
// command execution (see
// go/docs/autoscaler_v2_cloud_provider_implementation.md).
//
// The ssh target host is the cloud instance id: with the local provider the
// cloud instance id is the node IP (node_id is overloaded to be node_ip,
// aligned with Python LocalNodeProvider).
type SSHRayInstaller struct {
	sshUser                string
	sshPrivateKeyPath      string
	workerStartRayCommands []string
	sshPort                string
	dialTimeout            time.Duration
	commandTimeout         time.Duration
}

// NewSSHRayInstaller creates the installer from the autoscaling config:
// ssh credentials from the "auth" section (ssh_user defaults to ubuntu per
// the schema; ssh_private_key is the path of the private key file - when
// absent, an ssh agent is used), commands from
// "worker_start_ray_commands" (falling back to the local provider defaults).
func NewSSHRayInstaller(config *instance_manager.AutoscalingConfig) *SSHRayInstaller {
	authConfig := autoscaler.GetValueWithDefault[map[string]interface{}](
		config.Configs, map[string]interface{}{}, "auth")
	commands := toStringSlice(config.Configs["worker_start_ray_commands"])
	if len(commands) == 0 {
		commands = defaultWorkerStartRayCommands
	}
	return &SSHRayInstaller{
		sshUser:                autoscaler.GetValueWithDefault[string](authConfig, defaultSSHUser, "ssh_user"),
		sshPrivateKeyPath:      autoscaler.GetValueWithDefault[string](authConfig, "", "ssh_private_key"),
		workerStartRayCommands: commands,
		sshPort:                "22",
		dialTimeout:            defaultSSHDialTimeout,
		commandTimeout:         defaultSSHCommandTimeout,
	}
}

// InstallRay starts Ray on the target instance over ssh.
func (i *SSHRayInstaller) InstallRay(instance *proto.Instance, headNodeIP string) error {
	nodeIP := instance.GetCloudInstanceId()
	if nodeIP == "" {
		return fmt.Errorf("instance %s has no cloud instance id to ssh into", instance.InstanceId)
	}

	client, agentConn, err := i.dial(nodeIP)
	if agentConn != nil {
		defer agentConn.Close()
	}
	if err != nil {
		return err
	}
	defer client.Close()

	for _, command := range buildInstallCommands(instance, headNodeIP, i.workerStartRayCommands) {
		if err := i.runCommand(client, command); err != nil {
			return fmt.Errorf("failed to run command %q on node %s: %w", command, nodeIP, err)
		}
	}
	log.Log.Info("Installed ray on node.",
		"node", nodeIP, "instanceId", instance.InstanceId, "headNodeIP", headNodeIP)
	return nil
}

// dial establishes the ssh connection. Auth: private key file when
// configured, otherwise the ssh agent (SSH_AUTH_SOCK).
func (i *SSHRayInstaller) dial(nodeIP string) (*ssh.Client, net.Conn, error) {
	var authMethods []ssh.AuthMethod
	var agentConn net.Conn
	if i.sshPrivateKeyPath != "" {
		key, err := os.ReadFile(i.sshPrivateKeyPath)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to read ssh private key %s: %w", i.sshPrivateKeyPath, err)
		}
		signer, err := ssh.ParsePrivateKey(key)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to parse ssh private key %s: %w", i.sshPrivateKeyPath, err)
		}
		authMethods = append(authMethods, ssh.PublicKeys(signer))
	} else if socket := os.Getenv("SSH_AUTH_SOCK"); socket != "" {
		conn, err := net.Dial("unix", socket)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to connect to ssh agent %s: %w", socket, err)
		}
		agentConn = conn
		authMethods = append(authMethods, ssh.PublicKeysCallback(agent.NewClient(conn).Signers))
	}
	if len(authMethods) == 0 {
		return nil, nil, fmt.Errorf(
			"no ssh auth method available for node %s: configure auth.ssh_private_key or run an ssh agent", nodeIP)
	}

	client, err := ssh.Dial("tcp", net.JoinHostPort(nodeIP, i.sshPort), &ssh.ClientConfig{
		User: i.sshUser,
		Auth: authMethods,
		// No host key verification, aligned with the Python command runner
		// (StrictHostKeyChecking=no).
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		Timeout:         i.dialTimeout,
	})
	if err != nil {
		return nil, agentConn, fmt.Errorf("failed to ssh into node %s: %w", nodeIP, err)
	}
	return client, agentConn, nil
}

// runCommand runs a single command on the ssh client with a timeout.
func (i *SSHRayInstaller) runCommand(client *ssh.Client, command string) error {
	session, err := client.NewSession()
	if err != nil {
		return err
	}
	defer session.Close()

	done := make(chan error, 1)
	go func() {
		output, err := session.CombinedOutput(command)
		if err != nil {
			done <- fmt.Errorf("%w (output: %s)", err, output)
			return
		}
		log.Log.V(1).Info("Ray install command finished.", "command", command, "output", string(output))
		done <- nil
	}()

	select {
	case err := <-done:
		return err
	case <-time.After(i.commandTimeout):
		// Closing the session unblocks the pending command run.
		session.Close()
		return fmt.Errorf("command timed out after %s: %s", i.commandTimeout, command)
	}
}

// buildInstallCommands prefixes each worker start command with the env vars
// the default commands rely on (RAY_HEAD_IP), aligned with the Python
// with_envs/with_head_node_ip helpers.
func buildInstallCommands(instance *proto.Instance, headNodeIP string, commands []string) []string {
	if len(commands) == 0 {
		commands = defaultWorkerStartRayCommands
	}
	envPrefix := fmt.Sprintf("RAY_HEAD_IP=%s RAY_CLOUD_INSTANCE_ID=%s RAY_NODE_TYPE_NAME=%s",
		headNodeIP, instance.GetCloudInstanceId(), instance.GetInstanceType())
	result := make([]string, 0, len(commands))
	for _, command := range commands {
		result = append(result, envPrefix+" "+command)
	}
	return result
}

// toStringSlice extracts a []string from a YAML-parsed config value
// ([]string after JSON decoding, []interface{} after YAML decoding).
func toStringSlice(value interface{}) []string {
	switch typed := value.(type) {
	case []string:
		return typed
	case []interface{}:
		result := make([]string, 0, len(typed))
		for _, item := range typed {
			if s, ok := item.(string); ok {
				result = append(result, s)
			}
		}
		return result
	}
	return nil
}
