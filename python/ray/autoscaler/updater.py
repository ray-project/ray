from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

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


class NodeUpdater(object):
    """A process for syncing files and running init commands on a node."""

    def __init__(
            self, node_id, provider_config, auth_config, cluster_name,
            file_mounts, init_cmds, runtime_hash, redirect_output=True,
            process_runner=subprocess):
        self.daemon = True
        self.process_runner = process_runner
        self.provider = get_node_provider(provider_config, cluster_name)
        self.ssh_private_key = auth_config["ssh_private_key"]
        self.ssh_user = auth_config["ssh_user"]
        self.ssh_ip = self.provider.external_ip(node_id)
        self.node_id = node_id
        self.file_mounts = file_mounts
        self.init_cmds = init_cmds
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

    def run(self):
        print("NodeUpdater: Updating {} to {}, logging to {}".format(
            self.node_id, self.runtime_hash, self.output_name))
        try:
            self.do_update()
        except Exception as e:
            print(
                "NodeUpdater: Error updating {}, "
                "see {} for remote logs".format(e, self.output_name),
                file=self.stdout)
            self.provider.set_node_tags(
                self.node_id, {TAG_RAY_NODE_STATUS: "UpdateFailed"})
            if self.logfile is not None:
                print(
                    "----- BEGIN REMOTE LOGS -----\n" +
                    open(self.logfile.name).read() +
                    "\n----- END REMOTE LOGS -----")
            raise e
        self.provider.set_node_tags(
            self.node_id, {
                TAG_RAY_NODE_STATUS: "Up-to-date",
                TAG_RAY_RUNTIME_CONFIG: self.runtime_hash
            })
        print(
            "NodeUpdater: Applied config {} to node {}".format(
                self.runtime_hash, self.node_id),
            file=self.stdout)

    def do_update(self):
        self.provider.set_node_tags(
            self.node_id, {TAG_RAY_NODE_STATUS: "WaitingForSSH"})
        deadline = time.monotonic() + NODE_START_WAIT_S
        while time.monotonic() < deadline and \
                not self.provider.is_terminated(self.node_id):
            print(
                "NodeUpdater: Waiting for IP of {}...".format(self.node_id),
                file=self.stdout)
            self.ssh_ip = self.provider.external_ip(self.node_id)
            if self.ssh_ip is not None:
                break
            time.sleep(5)
        assert self.ssh_ip is not None, "Unable to find IP of node"
        while time.monotonic() < deadline and \
                not self.provider.is_terminated(self.node_id):
            try:
                print(
                    "NodeUpdater: Waiting for SSH to {}...".format(
                        self.node_id),
                    file=self.stdout)
                if not self.provider.is_running(self.node_id):
                    raise Exception()
                self.ssh_cmd(
                    "uptime",
                    connect_timeout=5, redirect=open("/dev/null", "w"))
            except Exception as e:
                print(
                    "NodeUpdater: SSH not up, retrying: {}".format(e),
                    file=self.stdout)
                time.sleep(5)
            else:
                break
        assert not self.provider.is_terminated(self.node_id)
        self.provider.set_node_tags(
            self.node_id, {TAG_RAY_NODE_STATUS: "SyncingFiles"})
        for remote_path, local_path in self.file_mounts.items():
            print(
                "NodeUpdater: Syncing {} to {}...".format(
                    local_path, remote_path),
                file=self.stdout)
            assert os.path.exists(local_path)
            if os.path.isdir(local_path):
                if not local_path.endswith("/"):
                    local_path += "/"
                if not remote_path.endswith("/"):
                    remote_path += "/"
            self.ssh_cmd(
                "mkdir -p {}".format(os.path.dirname(remote_path)))
            self.process_runner.check_call([
                "rsync", "-e", "ssh -i {} ".format(self.ssh_private_key) +
                "-o ConnectTimeout=60s -o StrictHostKeyChecking=no",
                "--delete", "-avz", "{}".format(local_path),
                "{}@{}:{}".format(self.ssh_user, self.ssh_ip, remote_path)
            ], stdout=self.stdout, stderr=self.stderr)
        self.provider.set_node_tags(
            self.node_id, {TAG_RAY_NODE_STATUS: "RunningInitCmds"})
        for cmd in self.init_cmds:
            self.ssh_cmd(cmd, verbose=True)

    def ssh_cmd(self, cmd, connect_timeout=60, redirect=None, verbose=False):
        if verbose:
            print(
                "NodeUpdater: running {} on {}...".format(
                    cmd, self.ssh_ip),
                file=self.stdout)
        self.process_runner.check_call([
            "ssh", "-o", "ConnectTimeout={}s".format(connect_timeout),
            "-o", "StrictHostKeyChecking=no",
            "-i", self.ssh_private_key,
            "{}@{}".format(self.ssh_user, self.ssh_ip),
            cmd,
        ], stdout=redirect or self.stdout, stderr=redirect or self.stderr)


class NodeUpdaterProcess(NodeUpdater, Process):
    def __init__(self, *args, **kwargs):
        Process.__init__(self)
        NodeUpdater.__init__(self, *args, **kwargs)


class NodeUpdaterThread(NodeUpdater, Thread):
    def __init__(self, *args, **kwargs):
        Thread.__init__(self)
        NodeUpdater.__init__(self, *args, **kwargs)
        self.exitcode = 0
