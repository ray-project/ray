from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

try:  # py3
    from shlex import quote
except ImportError:  # py2
    from pipes import quote
import logging
import os
import subprocess
import sys
import time

from threading import Thread
from getpass import getuser

from ray.autoscaler.tags import TAG_RAY_NODE_STATUS, TAG_RAY_RUNTIME_CONFIG
from ray.autoscaler.log_timer import LogTimer

logger = logging.getLogger(__name__)

# How long to wait for a node to start, in seconds
NODE_START_WAIT_S = 300
SSH_CHECK_INTERVAL = 5
CONTROL_PATH_MAX_LENGTH = 70


def get_default_ssh_options(private_key, connect_timeout, ssh_control_path):
    OPTS = [
        ("ConnectTimeout", "{}s".format(connect_timeout)),
        ("StrictHostKeyChecking", "no"),
        ("ControlMaster", "auto"),
        ("ControlPath", "{}/%C".format(ssh_control_path)),
        ("ControlPersist", "10s"),
    ]

    return ["-i", private_key] + [
        x for y in (["-o", "{}={}".format(k, v)] for k, v in OPTS) for x in y
    ]


class NodeUpdater(object):
    """A process for syncing files and running init commands on a node."""

    def __init__(self,
                 node_id,
                 provider_config,
                 provider,
                 auth_config,
                 cluster_name,
                 file_mounts,
                 initialization_commands,
                 setup_commands,
                 runtime_hash,
                 process_runner=subprocess,
                 use_internal_ip=False):

        ssh_control_path = "/tmp/{}_ray_ssh_sockets/{}".format(
            getuser(), cluster_name)[:CONTROL_PATH_MAX_LENGTH]

        self.daemon = True
        self.process_runner = process_runner
        self.node_id = node_id
        self.use_internal_ip = (use_internal_ip or provider_config.get(
            "use_internal_ips", False))
        self.provider = provider
        self.ssh_private_key = auth_config["ssh_private_key"]
        self.ssh_user = auth_config["ssh_user"]
        self.ssh_control_path = ssh_control_path
        self.ssh_ip = None
        self.file_mounts = {
            remote: os.path.expanduser(local)
            for remote, local in file_mounts.items()
        }
        self.initialization_commands = initialization_commands
        self.setup_commands = setup_commands
        self.runtime_hash = runtime_hash

    def get_caller(self, check_error):
        if check_error:
            return self.process_runner.call
        else:
            return self.process_runner.check_call

    def get_node_ip(self):
        if self.use_internal_ip:
            return self.provider.internal_ip(self.node_id)
        else:
            return self.provider.external_ip(self.node_id)

    def wait_for_ip(self, deadline):
        while time.time() < deadline and \
                not self.provider.is_terminated(self.node_id):
            logger.info("NodeUpdater: "
                        "Waiting for IP of {}...".format(self.node_id))
            ip = self.get_node_ip()
            if ip is not None:
                return ip
            time.sleep(10)

        return None

    def set_ssh_ip_if_required(self):
        if self.ssh_ip is not None:
            return

        # We assume that this never changes.
        #   I think that's reasonable.
        deadline = time.time() + NODE_START_WAIT_S
        with LogTimer("NodeUpdater: {}: Got IP".format(self.node_id)):
            ip = self.wait_for_ip(deadline)
            assert ip is not None, "Unable to find IP of node"

        self.ssh_ip = ip

        # This should run before any SSH commands and therefore ensure that
        #   the ControlPath directory exists, allowing SSH to maintain
        #   persistent sessions later on.
        with open("/dev/null", "w") as redirect:
            self.get_caller(False)(
                ["mkdir", "-p", self.ssh_control_path],
                stdout=redirect,
                stderr=redirect)

            self.get_caller(False)(
                ["chmod", "0700", self.ssh_control_path],
                stdout=redirect,
                stderr=redirect)

    def run(self):
        logger.info("NodeUpdater: "
                    "{}: Updating to {}".format(self.node_id,
                                                self.runtime_hash))
        try:
            m = "{}: Applied config {}".format(self.node_id, self.runtime_hash)
            with LogTimer("NodeUpdater: {}".format(m)):
                self.do_update()
        except Exception as e:
            error_str = str(e)
            if hasattr(e, "cmd"):
                error_str = "(Exit Status {}) {}".format(
                    e.returncode, " ".join(e.cmd))
            logger.error("NodeUpdater: "
                         "{}: Error updating {}".format(
                             self.node_id, error_str))
            self.provider.set_node_tags(self.node_id,
                                        {TAG_RAY_NODE_STATUS: "update-failed"})
            raise e

        self.provider.set_node_tags(
            self.node_id, {
                TAG_RAY_NODE_STATUS: "up-to-date",
                TAG_RAY_RUNTIME_CONFIG: self.runtime_hash
            })

        self.exitcode = 0

    def wait_for_ssh(self, deadline):
        logger.info("NodeUpdater: "
                    "{}: Waiting for SSH...".format(self.node_id))

        while time.time() < deadline and \
                not self.provider.is_terminated(self.node_id):
            try:
                logger.debug("NodeUpdater: "
                             "{}: Waiting for SSH...".format(self.node_id))

                # Setting redirect=False allows the user to see errors like
                # unix_listener: path "/tmp/rkn_ray_ssh_sockets/..." too long
                # for Unix domain socket.
                self.ssh_cmd("uptime", connect_timeout=5, redirect=False)

                return True

            except Exception as e:
                retry_str = str(e)
                if hasattr(e, "cmd"):
                    retry_str = "(Exit Status {}): {}".format(
                        e.returncode, " ".join(e.cmd))
                logger.debug("NodeUpdater: "
                             "{}: SSH not up, retrying: {}".format(
                                 self.node_id, retry_str))
                time.sleep(SSH_CHECK_INTERVAL)

        return False

    def sync_file_mounts(self, sync_cmd):
        # Rsync file mounts
        for remote_path, local_path in self.file_mounts.items():
            assert os.path.exists(local_path), local_path
            if os.path.isdir(local_path):
                if not local_path.endswith("/"):
                    local_path += "/"
                if not remote_path.endswith("/"):
                    remote_path += "/"

            m = "{}: Synced {} to {}".format(self.node_id, local_path,
                                             remote_path)
            with LogTimer("NodeUpdater {}".format(m)):
                self.ssh_cmd(
                    "mkdir -p {}".format(os.path.dirname(remote_path)),
                    redirect=None,
                )
                sync_cmd(local_path, remote_path, redirect=None)

    def do_update(self):
        self.provider.set_node_tags(self.node_id,
                                    {TAG_RAY_NODE_STATUS: "waiting-for-ssh"})

        deadline = time.time() + NODE_START_WAIT_S
        self.set_ssh_ip_if_required()

        # Wait for SSH access
        with LogTimer("NodeUpdater: " "{}: Got SSH".format(self.node_id)):
            ssh_ok = self.wait_for_ssh(deadline)
            assert ssh_ok, "Unable to SSH to node"

        self.provider.set_node_tags(self.node_id,
                                    {TAG_RAY_NODE_STATUS: "syncing-files"})
        self.sync_file_mounts(self.rsync_up)

        # Run init commands
        self.provider.set_node_tags(self.node_id,
                                    {TAG_RAY_NODE_STATUS: "setting-up"})
        m = "{}: Initialization commands completed".format(self.node_id)
        with LogTimer("NodeUpdater: {}".format(m)):
            for cmd in self.initialization_commands:
                self.ssh_cmd(cmd)

        m = "{}: Setup commands completed".format(self.node_id)
        with LogTimer("NodeUpdater: {}".format(m)):
            for cmd in self.setup_commands:
                self.ssh_cmd(cmd)

    def rsync_up(self, source, target, redirect=None, check_error=True):
        logger.info("NodeUpdater: "
                    "{}: Syncing {} to {}...".format(self.node_id, source,
                                                     target))
        self.set_ssh_ip_if_required()
        self.get_caller(check_error)(
            [
                "rsync", "-e", " ".join(["ssh"] + get_default_ssh_options(
                    self.ssh_private_key, 120, self.ssh_control_path)), "-avz",
                source, "{}@{}:{}".format(self.ssh_user, self.ssh_ip, target)
            ],
            stdout=redirect or sys.stdout,
            stderr=redirect or sys.stderr)

    def rsync_down(self, source, target, redirect=None, check_error=True):
        logger.info("NodeUpdater: "
                    "{}: Syncing {} from {}...".format(self.node_id, source,
                                                       target))
        self.set_ssh_ip_if_required()
        self.get_caller(check_error)(
            [
                "rsync", "-e", " ".join(["ssh"] + get_default_ssh_options(
                    self.ssh_private_key, 120, self.ssh_control_path)), "-avz",
                "{}@{}:{}".format(self.ssh_user, self.ssh_ip, source), target
            ],
            stdout=redirect or sys.stdout,
            stderr=redirect or sys.stderr)

    def ssh_cmd(self,
                cmd,
                connect_timeout=120,
                redirect=None,
                allocate_tty=False,
                emulate_interactive=True,
                expect_error=False,
                port_forward=None):

        self.set_ssh_ip_if_required()

        logger.info("NodeUpdater: Running {} on {}...".format(
            cmd, self.ssh_ip))
        ssh = ["ssh"]
        if allocate_tty:
            ssh.append("-tt")
        if emulate_interactive:
            force_interactive = (
                "true && source ~/.bashrc && "
                "export OMP_NUM_THREADS=1 PYTHONWARNINGS=ignore && ")
            cmd = "bash --login -c -i {}".format(
                quote(force_interactive + cmd))

        if port_forward is None:
            ssh_opt = []
        else:
            ssh_opt = [
                "-L", "{}:localhost:{}".format(port_forward, port_forward)
            ]

        self.get_caller(expect_error)(
            ssh + ssh_opt + get_default_ssh_options(
                self.ssh_private_key, connect_timeout, self.ssh_control_path) +
            ["{}@{}".format(self.ssh_user, self.ssh_ip), cmd],
            stdout=redirect or sys.stdout,
            stderr=redirect or sys.stderr)


class NodeUpdaterThread(NodeUpdater, Thread):
    def __init__(self, *args, **kwargs):
        Thread.__init__(self)
        NodeUpdater.__init__(self, *args, **kwargs)
        self.exitcode = -1
