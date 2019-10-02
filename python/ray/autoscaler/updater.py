from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

try:  # py3
    from shlex import quote
except ImportError:  # py2
    from pipes import quote
import hashlib
import logging
import os
import subprocess
import sys
import time

from threading import Thread
from getpass import getuser

from ray.autoscaler.tags import TAG_RAY_NODE_STATUS, TAG_RAY_RUNTIME_CONFIG, \
    STATUS_UP_TO_DATE, STATUS_UPDATE_FAILED, STATUS_WAITING_FOR_SSH, \
    STATUS_SETTING_UP, STATUS_SYNCING_FILES
from ray.autoscaler.log_timer import LogTimer

logger = logging.getLogger(__name__)

# How long to wait for a node to start, in seconds
NODE_START_WAIT_S = 300
READY_CHECK_INTERVAL = 5
HASH_MAX_LENGTH = 10
KUBECTL_RSYNC = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "kubernetes/kubectl-rsync.sh")


def with_interactive(cmd):
    force_interactive = ("true && source ~/.bashrc && "
                         "export OMP_NUM_THREADS=1 PYTHONWARNINGS=ignore && ")
    return ["bash", "--login", "-c", "-i", force_interactive + cmd]


class KubernetesCommandRunner(object):
    def __init__(self, log_prefix, namespace, node_id, auth_config,
                 process_runner):

        self.log_prefix = log_prefix
        self.process_runner = process_runner
        self.node_id = node_id
        self.namespace = namespace
        self.kubectl = ["kubectl", "-n", self.namespace]

    def run(self,
            cmd,
            timeout=120,
            redirect=None,
            allocate_tty=False,
            exit_on_fail=False,
            port_forward=None):

        logger.info(self.log_prefix + "Running {}...".format(cmd))

        if port_forward:
            port_forward_cmd = self.kubectl + [
                "port-forward", self.node_id,
                str(port_forward)
            ]
            port_forward_process = subprocess.Popen(port_forward_cmd)
            # Give port-forward a grace period to run and print output before
            # running the actual command. This is a little ugly, but it should
            # work in most scenarios and nothing should go very wrong if the
            # command starts running before the port forward starts.
            time.sleep(1)

        final_cmd = self.kubectl + [
            "exec", "-it" if allocate_tty else "-i", self.node_id, "--"
        ] + with_interactive(cmd)
        try:
            self.process_runner.check_call(
                final_cmd, stdout=redirect, stderr=redirect)
        except subprocess.CalledProcessError:
            if exit_on_fail:
                quoted_cmd = " ".join(final_cmd[:-1] + [quote(final_cmd[-1])])
                logger.error(self.log_prefix +
                             "Command failed: \n\n  {}\n".format(quoted_cmd))
                sys.exit(1)
            else:
                raise
        finally:
            # Clean up the port forward process. First, try to let it exit
            # gracefull with SIGTERM. If that doesn't work after 1s, send
            # SIGKILL.
            if port_forward:
                port_forward_process.terminate()
                for _ in range(10):
                    time.sleep(0.1)
                    port_forward_process.poll()
                    if port_forward_process.returncode:
                        break
                    logger.info(self.log_prefix +
                                "Waiting for port forward to die...")
                else:
                    logger.warning(self.log_prefix +
                                   "Killing port forward with SIGKILL.")
                    port_forward_process.kill()

    def run_rsync_up(self, source, target, redirect=None):
        if target.startswith("~"):
            target = "/root" + target[1:]

        try:
            self.process_runner.check_call(
                [
                    KUBECTL_RSYNC,
                    "-avz",
                    source,
                    "{}@{}:{}".format(self.node_id, self.namespace, target),
                ],
                stdout=redirect,
                stderr=redirect)
        except Exception as e:
            logger.warning(self.log_prefix +
                           "rsync failed: '{}'. Falling back to 'kubectl cp'"
                           .format(e))
            self.process_runner.check_call(
                self.kubectl + [
                    "cp", source, "{}/{}:{}".format(self.namespace,
                                                    self.node_id, target)
                ],
                stdout=redirect,
                stderr=redirect)

    def run_rsync_down(self, source, target, redirect=None):
        if target.startswith("~"):
            target = "/root" + target[1:]

        try:
            self.process_runner.check_call(
                [
                    KUBECTL_RSYNC,
                    "-avz",
                    "{}@{}:{}".format(self.node_id, self.namespace, source),
                    target,
                ],
                stdout=redirect,
                stderr=redirect)
        except Exception as e:
            logger.warning(self.log_prefix +
                           "rsync failed: '{}'. Falling back to 'kubectl cp'"
                           .format(e))
            self.process_runner.check_call(
                self.kubectl + [
                    "cp", "{}/{}:{}".format(self.namespace, self.node_id,
                                            source), target
                ],
                stdout=redirect,
                stderr=redirect)

    def remote_shell_command_str(self):
        return "{} exec -it {} bash".format(" ".join(self.kubectl),
                                            self.node_id)


class SSHCommandRunner(object):
    def __init__(self, log_prefix, node_id, provider, auth_config,
                 cluster_name, process_runner, use_internal_ip):

        ssh_control_hash = hashlib.md5(cluster_name.encode()).hexdigest()
        ssh_user_hash = hashlib.md5(getuser().encode()).hexdigest()
        ssh_control_path = "/tmp/ray_ssh_{}/{}".format(
            ssh_user_hash[:HASH_MAX_LENGTH],
            ssh_control_hash[:HASH_MAX_LENGTH])

        self.log_prefix = log_prefix
        self.process_runner = process_runner
        self.node_id = node_id
        self.use_internal_ip = use_internal_ip
        self.provider = provider
        self.ssh_private_key = auth_config["ssh_private_key"]
        self.ssh_user = auth_config["ssh_user"]
        self.ssh_control_path = ssh_control_path
        self.ssh_ip = None

    def get_default_ssh_options(self, connect_timeout):
        OPTS = [
            ("ConnectTimeout", "{}s".format(connect_timeout)),
            ("StrictHostKeyChecking", "no"),
            ("ControlMaster", "auto"),
            ("ControlPath", "{}/%C".format(self.ssh_control_path)),
            ("ControlPersist", "10s"),
        ]

        return ["-i", self.ssh_private_key] + [
            x for y in (["-o", "{}={}".format(k, v)] for k, v in OPTS)
            for x in y
        ]

    def get_node_ip(self):
        if self.use_internal_ip:
            return self.provider.internal_ip(self.node_id)
        else:
            return self.provider.external_ip(self.node_id)

    def wait_for_ip(self, deadline):
        while time.time() < deadline and \
                not self.provider.is_terminated(self.node_id):
            logger.info(self.log_prefix + "Waiting for IP...")
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
        with LogTimer(self.log_prefix + "Got IP"):
            ip = self.wait_for_ip(deadline)
            assert ip is not None, "Unable to find IP of node"

        self.ssh_ip = ip

        # This should run before any SSH commands and therefore ensure that
        #   the ControlPath directory exists, allowing SSH to maintain
        #   persistent sessions later on.
        with open("/dev/null", "w") as redirect:
            try:
                self.process_runner.check_call(
                    ["mkdir", "-p", self.ssh_control_path],
                    stdout=redirect,
                    stderr=redirect)
            except subprocess.CalledProcessError as e:
                logger.warning(e)

            try:
                self.process_runner.check_call(
                    ["chmod", "0700", self.ssh_control_path],
                    stdout=redirect,
                    stderr=redirect)
            except subprocess.CalledProcessError as e:
                logger.warning(self.log_prefix + str(e))

    def run(self,
            cmd,
            timeout=120,
            redirect=None,
            allocate_tty=False,
            exit_on_fail=False,
            port_forward=None):

        self.set_ssh_ip_if_required()

        logger.info(self.log_prefix +
                    "Running {} on {}...".format(cmd, self.ssh_ip))
        ssh = ["ssh"]
        if allocate_tty:
            ssh.append("-tt")

        if port_forward:
            ssh += ["-L", "{}:localhost:{}".format(port_forward, port_forward)]

        final_cmd = ssh + self.get_default_ssh_options(timeout) + [
            "{}@{}".format(self.ssh_user, self.ssh_ip)
        ] + with_interactive(cmd)
        try:
            self.process_runner.check_call(
                final_cmd, stdout=redirect, stderr=redirect)
        except subprocess.CalledProcessError:
            if exit_on_fail:
                quoted_cmd = " ".join(final_cmd[:-1] + [quote(final_cmd[-1])])
                logger.error(self.log_prefix +
                             "Command failed: \n\n  {}\n".format(quoted_cmd))
                sys.exit(1)
            else:
                raise

    def run_rsync_up(self, source, target, redirect=None):
        self.set_ssh_ip_if_required()
        self.process_runner.check_call(
            [
                "rsync", "--rsh",
                " ".join(["ssh"] + self.get_default_ssh_options(120)), "-avz",
                source, "{}@{}:{}".format(self.ssh_user, self.ssh_ip, target)
            ],
            stdout=redirect,
            stderr=redirect)

    def rsync_down(self, source, target, redirect=None):
        self.set_ssh_ip_if_required()
        self.process_runner.check_call(
            [
                "rsync", "--rsh",
                " ".join(["ssh"] + self.get_default_ssh_options(120)), "-avz",
                "{}@{}:{}".format(self.ssh_user, self.ssh_ip, source), target
            ],
            stdout=redirect,
            stderr=redirect)

    def remote_shell_command_str(self):
        return "ssh -i {} {}@{}\n".format(self.ssh_private_key, self.ssh_user,
                                          self.ssh_ip)


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
                 ray_start_commands,
                 runtime_hash,
                 process_runner=subprocess,
                 use_internal_ip=False):

        self.log_prefix = "NodeUpdater: {}: ".format(node_id)
        if provider_config["type"] == "kubernetes":
            self.cmd_runner = KubernetesCommandRunner(
                self.log_prefix, provider.namespace, node_id, auth_config,
                process_runner)
        else:
            use_internal_ip = (use_internal_ip or provider_config.get(
                "use_internal_ips", False))
            self.cmd_runner = SSHCommandRunner(
                self.log_prefix, node_id, provider, auth_config, cluster_name,
                process_runner, use_internal_ip)

        self.daemon = True
        self.process_runner = process_runner
        self.node_id = node_id
        self.provider = provider
        self.file_mounts = {
            remote: os.path.expanduser(local)
            for remote, local in file_mounts.items()
        }
        self.initialization_commands = initialization_commands
        self.setup_commands = setup_commands
        self.ray_start_commands = ray_start_commands
        self.runtime_hash = runtime_hash

    def run(self):
        logger.info(self.log_prefix +
                    "Updating to {}".format(self.runtime_hash))
        try:
            with LogTimer(self.log_prefix +
                          "Applied config {}".format(self.runtime_hash)):
                self.do_update()
        except Exception as e:
            error_str = str(e)
            if hasattr(e, "cmd"):
                error_str = "(Exit Status {}) {}".format(
                    e.returncode, " ".join(e.cmd))
            logger.error(self.log_prefix +
                         "Error updating {}".format(error_str))
            self.provider.set_node_tags(
                self.node_id, {TAG_RAY_NODE_STATUS: STATUS_UPDATE_FAILED})
            raise e

        self.provider.set_node_tags(
            self.node_id, {
                TAG_RAY_NODE_STATUS: STATUS_UP_TO_DATE,
                TAG_RAY_RUNTIME_CONFIG: self.runtime_hash
            })

        self.exitcode = 0

    def sync_file_mounts(self, sync_cmd):
        # Rsync file mounts
        for remote_path, local_path in self.file_mounts.items():
            assert os.path.exists(local_path), local_path
            if os.path.isdir(local_path):
                if not local_path.endswith("/"):
                    local_path += "/"
                if not remote_path.endswith("/"):
                    remote_path += "/"

            with LogTimer(self.log_prefix +
                          "Synced {} to {}".format(local_path, remote_path)):
                self.cmd_runner.run("mkdir -p {}".format(
                    os.path.dirname(remote_path)))
                sync_cmd(local_path, remote_path, redirect=None)

    def wait_ready(self, deadline):
        with LogTimer(self.log_prefix + "Got remote shell"):
            logger.info(self.log_prefix + "Waiting for remote shell...")

            while time.time() < deadline and \
                    not self.provider.is_terminated(self.node_id):
                try:
                    logger.debug(self.log_prefix +
                                 "Waiting for remote shell...")

                    # Setting redirect=False allows the user to see errors like
                    # unix_listener: path "/tmp/rkn_ray_ssh_sockets/..." too
                    # long for Unix domain socket.
                    self.cmd_runner.run("uptime", timeout=5, redirect=False)

                    return True

                except Exception as e:
                    retry_str = str(e)
                    if hasattr(e, "cmd"):
                        retry_str = "(Exit Status {}): {}".format(
                            e.returncode, " ".join(e.cmd))
                    logger.debug(self.log_prefix +
                                 "Node not up, retrying: {}".format(retry_str))
                    time.sleep(READY_CHECK_INTERVAL)

        assert False, "Unable to connect to node"

    def do_update(self):
        self.provider.set_node_tags(
            self.node_id, {TAG_RAY_NODE_STATUS: STATUS_WAITING_FOR_SSH})

        deadline = time.time() + NODE_START_WAIT_S
        self.wait_ready(deadline)

        node_tags = self.provider.node_tags(self.node_id)
        if node_tags.get(TAG_RAY_RUNTIME_CONFIG) == self.runtime_hash:
            logger.info(
                "NodeUpdater: {} already up-to-date, skip to ray start".format(
                    self.node_id))
        else:
            self.provider.set_node_tags(
                self.node_id, {TAG_RAY_NODE_STATUS: STATUS_SYNCING_FILES})
            self.sync_file_mounts(self.rsync_up)

            # Run init commands
            self.provider.set_node_tags(
                self.node_id, {TAG_RAY_NODE_STATUS: STATUS_SETTING_UP})
            with LogTimer(self.log_prefix +
                          "Initialization commands completed"):
                for cmd in self.initialization_commands:
                    self.cmd_runner.run(cmd)

            with LogTimer(self.log_prefix + "Setup commands completed"):
                for cmd in self.setup_commands:
                    self.cmd_runner.run(cmd)

        with LogTimer(self.log_prefix + "Ray start commands completed"):
            for cmd in self.ray_start_commands:
                self.cmd_runner.run(cmd)

    def rsync_up(self, source, target, redirect=None):
        logger.info(self.log_prefix +
                    "Syncing {} to {}...".format(source, target))
        self.cmd_runner.run_rsync_up(source, target, redirect=None)

    def rsync_down(self, source, target, redirect=None):
        logger.info(self.log_prefix +
                    "Syncing {} from {}...".format(source, target))
        self.cmd_runner.run_rsync_down(source, target, redirect=None)


class NodeUpdaterThread(NodeUpdater, Thread):
    def __init__(self, *args, **kwargs):
        Thread.__init__(self)
        NodeUpdater.__init__(self, *args, **kwargs)
        self.exitcode = -1
