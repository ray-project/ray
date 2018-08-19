from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

try:  # py3
    from shlex import quote
except ImportError:  # py2
    from pipes import quote
import os
import subprocess
import sys
import tempfile
import time

from multiprocessing import Process
from threading import Thread

from ray.autoscaler.node_provider import get_node_provider
from ray.autoscaler.tags import TAG_RAY_NODE_STATUS, TAG_RAY_RUNTIME_CONFIG

# How long to wait for a node to start, in seconds
NODE_START_WAIT_S = 300
SSH_CHECK_INTERVAL = 5


def pretty_cmd(cmd_str):
    return "\n\n\t{}\n\n".format(cmd_str)


class NodeUpdater(object):
    """A process for syncing files and running init commands on a node."""

    def __init__(self,
                 node_id,
                 provider_config,
                 auth_config,
                 cluster_name,
                 file_mounts,
                 setup_cmds,
                 runtime_hash,
                 redirect_output=True,
                 process_runner=subprocess,
                 use_internal_ip=False):
        self.daemon = True
        self.process_runner = process_runner
        self.node_id = node_id
        self.use_internal_ip = use_internal_ip
        self.provider = get_node_provider(provider_config, cluster_name)
        self.ssh_private_key = auth_config["ssh_private_key"]
        self.ssh_user = auth_config["ssh_user"]
        self.ssh_ip = self.get_node_ip()
        self.file_mounts = {
            remote: os.path.expanduser(local)
            for remote, local in file_mounts.items()
        }
        self.setup_cmds = setup_cmds
        self.runtime_hash = runtime_hash
        if redirect_output:
            self.logfile = tempfile.NamedTemporaryFile(
                mode="w", prefix="node-updater-", delete=False)
            self.output_name = self.logfile.name
            self.stdout = self.logfile
            self.stderr = self.logfile
        else:
            self.logfile = None
            self.output_name = "(console)"
            self.stdout = sys.stdout
            self.stderr = sys.stderr

    def get_node_ip(self):
        if self.use_internal_ip:
            return self.provider.internal_ip(self.node_id)
        else:
            return self.provider.external_ip(self.node_id)

    def run(self):
        print("NodeUpdater: Updating {} to {}, logging to {}".format(
            self.node_id, self.runtime_hash, self.output_name))
        try:
            self.do_update()
        except Exception as e:
            error_str = str(e)
            if hasattr(e, "cmd"):
                error_str = "(Exit Status {}) {}".format(
                    e.returncode, pretty_cmd(" ".join(e.cmd)))
            print(
                "NodeUpdater: Error updating {}"
                "See {} for remote logs.".format(error_str, self.output_name),
                file=self.stdout)
            self.provider.set_node_tags(self.node_id,
                                        {TAG_RAY_NODE_STATUS: "update-failed"})
            if self.logfile is not None:
                print("----- BEGIN REMOTE LOGS -----\n" +
                      open(self.logfile.name).read() +
                      "\n----- END REMOTE LOGS -----")
            raise e
        self.provider.set_node_tags(
            self.node_id, {
                TAG_RAY_NODE_STATUS: "up-to-date",
                TAG_RAY_RUNTIME_CONFIG: self.runtime_hash
            })
        print(
            "NodeUpdater: Applied config {} to node {}".format(
                self.runtime_hash, self.node_id),
            file=self.stdout)

    def do_update(self):
        self.provider.set_node_tags(self.node_id,
                                    {TAG_RAY_NODE_STATUS: "waiting-for-ssh"})
        deadline = time.time() + NODE_START_WAIT_S

        # Wait for external IP
        while time.time() < deadline and \
                not self.provider.is_terminated(self.node_id):
            print(
                "NodeUpdater: Waiting for IP of {}...".format(self.node_id),
                file=self.stdout)
            self.ssh_ip = self.get_node_ip()
            if self.ssh_ip is not None:
                break
            time.sleep(10)
        assert self.ssh_ip is not None, "Unable to find IP of node"

        # Wait for SSH access
        ssh_ok = False
        while time.time() < deadline and \
                not self.provider.is_terminated(self.node_id):
            try:
                print(
                    "NodeUpdater: Waiting for SSH to {}...".format(
                        self.node_id),
                    file=self.stdout)
                if not self.provider.is_running(self.node_id):
                    raise Exception("Node not running yet...")
                self.ssh_cmd(
                    "uptime",
                    connect_timeout=5,
                    redirect=open("/dev/null", "w"))
                ssh_ok = True
            except Exception as e:
                retry_str = str(e)
                if hasattr(e, "cmd"):
                    retry_str = "(Exit Status {}): {}".format(
                        e.returncode, pretty_cmd(" ".join(e.cmd)))
                print(
                    "NodeUpdater: SSH not up, retrying: {}".format(retry_str),
                    file=self.stdout)
                time.sleep(SSH_CHECK_INTERVAL)
            else:
                break
        assert ssh_ok, "Unable to SSH to node"

        # Rsync file mounts
        self.provider.set_node_tags(self.node_id,
                                    {TAG_RAY_NODE_STATUS: "syncing-files"})
        for remote_path, local_path in self.file_mounts.items():
            print(
                "NodeUpdater: Syncing {} to {}...".format(
                    local_path, remote_path),
                file=self.stdout)
            assert os.path.exists(local_path), local_path
            if os.path.isdir(local_path):
                if not local_path.endswith("/"):
                    local_path += "/"
                if not remote_path.endswith("/"):
                    remote_path += "/"
            self.ssh_cmd("mkdir -p {}".format(os.path.dirname(remote_path)))
            self.process_runner.check_call(
                [
                    "rsync", "-e", "ssh -i {} ".format(self.ssh_private_key) +
                    "-o ConnectTimeout=120s -o StrictHostKeyChecking=no",
                    "--delete", "-avz", "{}".format(local_path),
                    "{}@{}:{}".format(self.ssh_user, self.ssh_ip, remote_path)
                ],
                stdout=self.stdout,
                stderr=self.stderr)

        # Run init commands
        self.provider.set_node_tags(self.node_id,
                                    {TAG_RAY_NODE_STATUS: "setting-up"})
        for cmd in self.setup_cmds:
            self.ssh_cmd(cmd, verbose=True)

    def ssh_cmd(self,
                cmd,
                connect_timeout=120,
                redirect=None,
                verbose=False,
                allocate_tty=False,
                emulate_interactive=True,
                expect_error=False):
        if verbose:
            print(
                "NodeUpdater: running {} on {}...".format(
                    pretty_cmd(cmd), self.ssh_ip),
                file=self.stdout)
        ssh = ["ssh"]
        if allocate_tty:
            ssh.append("-tt")
        if emulate_interactive:
            force_interactive = "set -i || true && source ~/.bashrc && "
            cmd = "bash --login -c {}".format(quote(force_interactive + cmd))
        if expect_error:
            call = self.process_runner.call
        else:
            call = self.process_runner.check_call
        call(
            ssh + [
                "-o", "ConnectTimeout={}s".format(connect_timeout), "-o",
                "StrictHostKeyChecking=no", "-i", self.ssh_private_key,
                "{}@{}".format(self.ssh_user, self.ssh_ip), cmd
            ],
            stdout=redirect or self.stdout,
            stderr=redirect or self.stderr)


class NodeUpdaterProcess(NodeUpdater, Process):
    def __init__(self, *args, **kwargs):
        Process.__init__(self)
        NodeUpdater.__init__(self, *args, **kwargs)


# Single-threaded version for unit tests
class NodeUpdaterThread(NodeUpdater, Thread):
    def __init__(self, *args, **kwargs):
        Thread.__init__(self)
        NodeUpdater.__init__(self, *args, **kwargs)
        self.exitcode = 0
