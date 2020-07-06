try:  # py3
    from shlex import quote
except ImportError:  # py2
    from pipes import quote
import click
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
from ray.autoscaler.docker import check_docker_running_cmd

from ray.autoscaler.cli_logger import cli_logger, CLILoggerError
import colorful as cf

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
    return ["bash", "--login", "-c", "-i", quote(force_interactive + cmd)]


class KubernetesCommandRunner:
    def __init__(self, log_prefix, namespace, node_id, auth_config,
                 process_runner):

        self.log_prefix = log_prefix
        self.process_runner = process_runner
        self.node_id = node_id
        self.namespace = namespace
        self.kubectl = ["kubectl", "-n", self.namespace]

    def run(self,
            cmd=None,
            timeout=120,
            exit_on_fail=False,
            port_forward=None,
            with_output=False):
        if cmd and port_forward:
            raise Exception(
                "exec with Kubernetes can't forward ports and execute"
                "commands together.")

        if port_forward:
            if not isinstance(port_forward, list):
                port_forward = [port_forward]
            port_forward_cmd = self.kubectl + [
                "port-forward",
                self.node_id,
            ] + [
                "{}:{}".format(local, remote) for local, remote in port_forward
            ]
            logger.info("Port forwarding with: {}".format(
                " ".join(port_forward_cmd)))
            port_forward_process = subprocess.Popen(port_forward_cmd)
            port_forward_process.wait()
            # We should never get here, this indicates that port forwarding
            # failed, likely because we couldn't bind to a port.
            pout, perr = port_forward_process.communicate()
            exception_str = " ".join(
                port_forward_cmd) + " failed with error: " + perr
            raise Exception(exception_str)
        else:
            final_cmd = self.kubectl + ["exec", "-it"]
            final_cmd += [
                self.node_id,
                "--",
            ]
            final_cmd += with_interactive(cmd)
            logger.info(self.log_prefix + "Running {}".format(final_cmd))
            try:
                if with_output:
                    return self.process_runner.check_output(
                        " ".join(final_cmd), shell=True)
                else:
                    self.process_runner.check_call(
                        " ".join(final_cmd), shell=True)
            except subprocess.CalledProcessError:
                if exit_on_fail:
                    quoted_cmd = " ".join(final_cmd[:-1] +
                                          [quote(final_cmd[-1])])
                    logger.error(
                        self.log_prefix +
                        "Command failed: \n\n  {}\n".format(quoted_cmd))
                    sys.exit(1)
                else:
                    raise

    def run_rsync_up(self, source, target):
        if target.startswith("~"):
            target = "/root" + target[1:]

        try:
            self.process_runner.check_call([
                KUBECTL_RSYNC,
                "-avz",
                source,
                "{}@{}:{}".format(self.node_id, self.namespace, target),
            ])
        except Exception as e:
            logger.warning(self.log_prefix +
                           "rsync failed: '{}'. Falling back to 'kubectl cp'"
                           .format(e))
            self.process_runner.check_call(self.kubectl + [
                "cp", source, "{}/{}:{}".format(self.namespace, self.node_id,
                                                target)
            ])

    def run_rsync_down(self, source, target):
        if target.startswith("~"):
            target = "/root" + target[1:]

        try:
            self.process_runner.check_call([
                KUBECTL_RSYNC,
                "-avz",
                "{}@{}:{}".format(self.node_id, self.namespace, source),
                target,
            ])
        except Exception as e:
            logger.warning(self.log_prefix +
                           "rsync failed: '{}'. Falling back to 'kubectl cp'"
                           .format(e))
            self.process_runner.check_call(self.kubectl + [
                "cp", "{}/{}:{}".format(self.namespace, self.node_id, source),
                target
            ])

    def remote_shell_command_str(self):
        return "{} exec -it {} bash".format(" ".join(self.kubectl),
                                            self.node_id)


class SSHCommandRunner:
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
            # Try fewer extraneous key pairs.
            ("IdentitiesOnly", "yes"),
            # Abort if port forwarding fails (instead of just printing to
            # stderr).
            ("ExitOnForwardFailure", "yes"),
            # Quickly kill the connection if network connection breaks (as
            # opposed to hanging/blocking).
            ("ServerAliveInterval", 5),
            ("ServerAliveCountMax", 3),
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
        # if we have IP do not print waiting info
        # ip = self.get_node_ip()
        # if ip is not None:
        #     cli_logger.keyvalue("Fetched IP", ip)
        #     return ip

        interval = 10
        with cli_logger.timed("Waiting for IP",
                              _tags=dict(interval=str(interval) + "s")):
            while time.time() < deadline and \
                    not self.provider.is_terminated(self.node_id):
                cli_logger.old_info(
                    logger, "{}Waiting for IP...", self.log_prefix)

                ip = self.get_node_ip()
                if ip is not None:
                    cli_logger.keyvalue("Received", ip)
                    return ip
                time.sleep(interval)

        return None

    def set_ssh_ip_if_required(self):
        if self.ssh_ip is not None:
            return

        # We assume that this never changes.
        #   I think that's reasonable.
        deadline = time.time() + NODE_START_WAIT_S
        with LogTimer(self.log_prefix + "Got IP"):
            ip = self.wait_for_ip(deadline)

            cli_logger.doassert(
                ip is not None,
                "Could not get node IP.") # todo: msg
            assert ip is not None, "Unable to find IP of node"

        self.ssh_ip = ip

        # This should run before any SSH commands and therefore ensure that
        #   the ControlPath directory exists, allowing SSH to maintain
        #   persistent sessions later on.
        try:
            os.makedirs(self.ssh_control_path, mode=0o700, exist_ok=True)
        except OSError as e:
            cli_logger.warning(e) # todo: msg
            cli_logger.old_warning(logger, e)

    def run(self,
            cmd,
            timeout=120,
            exit_on_fail=False,
            port_forward=None,
            with_output=False):

        self.set_ssh_ip_if_required()

        ssh = ["ssh", "-tt"]

        if port_forward:
            with cli_logger.group("Forwarding ports"):
                if not isinstance(port_forward, list):
                    port_forward = [port_forward]
                for local, remote in port_forward:
                    cli_logger.verbose(
                        "Forwarding port {} to port {} on localhost.",
                        cf.bold(local), cf.bold(remote)) # todo: msg
                    cli_logger.old_info(
                        logger,
                        "{}Forwarding {} -> localhost:{}",
                        self.log_prefix, local, remote)
                    ssh += ["-L", "{}:localhost:{}".format(remote, local)]

        final_cmd = ssh + self.get_default_ssh_options(timeout) + [
            "{}@{}".format(self.ssh_user, self.ssh_ip)
        ]
        if cmd:
            cli_logger.old_info(
                logger,
                "{}Running {}",
                self.log_prefix,
                " ".join(final_cmd))
            final_cmd += with_interactive(cmd)
        else:
            # We do this because `-o ControlMaster` causes the `-N` flag to
            # still create an interactive shell in some ssh versions.
            final_cmd.append(quote("while true; do sleep 86400; done"))


        # todo: add a flag for this, we might
        # wanna log commands with print sometimes
        cli_logger.verbose(
            "Running `{}`",
            cf.bold(cmd))
        with cli_logger.indented():
            cli_logger.very_verbose(
                "Full command is `{}`",
                cf.bold(" ".join(final_cmd)))

        try:
            # todo: catch and log
            # Warning: Permanently added '52.37.238.148' (ECDSA) to the list of known hosts.

            if with_output:
                return self.process_runner.check_output(final_cmd)
            else:
                self.process_runner.check_call(final_cmd)
        except subprocess.CalledProcessError as e:
            if not cli_logger.old_style:
                raise CLILoggerError(
                    "Command failed",
                    "ssh_command_failed",
                    dict(
                        code = e.returncode,
                        command = final_cmd,
                        stdout = e.output,
                        stderr = e.stderr))

            if exit_on_fail:
                quoted_cmd = " ".join(final_cmd[:-1] + [quote(final_cmd[-1])])
                raise click.ClickException(
                    "Command failed: \n\n  {}\n".format(quoted_cmd)) from None
            else:
                raise click.ClickException(
                    "SSH command Failed. See above for the output from the"
                    " failure.") from None

    def run_rsync_up(self, source, target):
        self.set_ssh_ip_if_required()

        command = [
            "rsync", "--rsh",
            " ".join(["ssh"] + self.get_default_ssh_options(120)), "-avz",
            source, "{}@{}:{}".format(self.ssh_user, self.ssh_ip, target)
        ]
        self.process_runner.check_call(command)

    def run_rsync_down(self, source, target):
        self.set_ssh_ip_if_required()

        command = [
            "rsync", "--rsh",
            " ".join(["ssh"] + self.get_default_ssh_options(120)), "-avz",
            "{}@{}:{}".format(self.ssh_user, self.ssh_ip, source), target
        ]
        self.process_runner.check_call(command)

    def remote_shell_command_str(self):
        return "ssh -o IdentitiesOnly=yes -i {} {}@{}\n".format(
            self.ssh_private_key, self.ssh_user, self.ssh_ip)


class DockerCommandRunner(SSHCommandRunner):
    def __init__(self, docker_config, **common_args):
        self.ssh_command_runner = SSHCommandRunner(**common_args)
        self.docker_name = docker_config["container_name"]
        self.docker_config = docker_config
        self.home_dir = None

    def run(self,
            cmd,
            timeout=120,
            exit_on_fail=False,
            port_forward=None,
            with_output=False):

        return self.ssh_command_runner.run(
            cmd,
            timeout=timeout,
            exit_on_fail=exit_on_fail,
            port_forward=port_forward,
            with_output=with_output)

    def check_container_status(self):
        no_exist = "not_present"
        cmd = check_docker_running_cmd(self.docker_name) + " ".join(
            ["||", "echo", quote(no_exist)])
        output = self.ssh_command_runner.run(
            cmd, with_output=True).decode("utf-8").strip()
        if no_exist in output:
            return False
        return output

    def run_rsync_up(self, source, target):
        self.ssh_command_runner.run_rsync_up(source, target)
        if self.check_container_status():
            self.ssh_command_runner.run("docker cp {} {}:{}".format(
                target, self.docker_name, self.docker_expand_user(target)))

    def run_rsync_down(self, source, target):
        self.ssh_command_runner.run("docker cp {}:{} {}".format(
            self.docker_name, self.docker_expand_user(source), source))
        self.ssh_command_runner.run_rsync_down(source, target)

    def remote_shell_command_str(self):
        inner_str = self.ssh_command_runner.remote_shell_command_str().replace(
            "ssh", "ssh -tt", 1).strip("\n")
        return inner_str + " docker exec -it {} /bin/bash\n".format(
            self.docker_name)

    def docker_expand_user(self, string):
        if string.find("~") == 0:
            if self.home_dir is None:
                self.home_dir = self.ssh_command_runner.run(
                    "docker exec {} env | grep HOME | cut -d'=' -f2".format(
                        self.docker_name),
                    with_output=True).decode("utf-8").strip()
            return string.replace("~", self.home_dir)
        else:
            return string


class NodeUpdater:
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
                 use_internal_ip=False,
                 docker_config=None):

        self.log_prefix = "NodeUpdater: {}: ".format(node_id)
        use_internal_ip = (use_internal_ip
                           or provider_config.get("use_internal_ips", False))
        self.cmd_runner = provider.get_command_runner(
            self.log_prefix, node_id, auth_config, cluster_name,
            process_runner, use_internal_ip, docker_config)

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
        cli_logger.old_info(
            logger,
            "{}Updating to {}",
            self.log_prefix, self.runtime_hash)

        try:
            with LogTimer(self.log_prefix +
                          "Applied config {}".format(self.runtime_hash)):
                self.do_update()
        except Exception as e:
            error_str = str(e)
            if hasattr(e, "cmd"):
                error_str = "(Exit Status {}) {}".format(
                    e.returncode, " ".join(e.cmd))

            self.provider.set_node_tags(
                self.node_id, {TAG_RAY_NODE_STATUS: STATUS_UPDATE_FAILED})
            cli_logger.error("New status: {}", cf.bold(STATUS_UPDATE_FAILED))

            cli_logger.old_error(
                logger,
                "{}Error executing: {}\n",
                self.log_prefix, error_str)

            cli_logger.error("!!!")
            if hasattr(e, "cmd"):
                cli_logger.error(
                    "Setup command `{}` failed with exit code {}. stderr:",
                    cf.bold(e.cmd),
                    e.returncode)
            else:
                cli_logger.verbose_error(vars(e), _no_format=True)
                cli_logger.error(str(e)) # todo: handle this better somehow?
            # todo: print stderr here
            cli_logger.error("!!!")
            cli_logger.newline()

            if isinstance(e, click.ClickException):
                # todo: why do we ignore this here
                return
            raise

        self.provider.set_node_tags(
            self.node_id, {
                TAG_RAY_NODE_STATUS: STATUS_UP_TO_DATE,
                TAG_RAY_RUNTIME_CONFIG: self.runtime_hash
            })
        cli_logger.keyvalue("New status", STATUS_UP_TO_DATE)

        self.exitcode = 0

    def sync_file_mounts(self, sync_cmd):
        nolog_paths = []
        if cli_logger.verbosity == 0:
            nolog_paths = [
                "~/ray_bootstrap_key.pem",
                "~/ray_bootstrap_config.yaml"
            ]

        # Rsync file mounts
        with cli_logger.group(
            "Processing file mounts",
            _numbered=("[]", 2, 5)):
            for remote_path, local_path in self.file_mounts.items():
                assert os.path.exists(local_path), local_path
                if os.path.isdir(local_path):
                    if not local_path.endswith("/"):
                        local_path += "/"
                    if not remote_path.endswith("/"):
                        remote_path += "/"

                with LogTimer(self.log_prefix +
                              "Synced {} to {}".format(
                                local_path, remote_path)):
                    self.cmd_runner.run("mkdir -p {}".format(
                        os.path.dirname(remote_path)))
                    sync_cmd(local_path, remote_path)

                    if remote_path not in nolog_paths:
                        # todo: timed here?
                        cli_logger.print(
                            "{} from {}",
                            cf.bold(remote_path),
                            cf.bold(local_path))

    def wait_ready(self, deadline):
        with cli_logger.group(
            "Waiting for SSH to become available",
            _numbered=("[]", 1, 5),
            _tags=dict(
                interval=str(READY_CHECK_INTERVAL) + "s"
            )):
            with LogTimer(self.log_prefix + "Got remote shell"):
                cli_logger.old_info(
                    logger, "{}Waiting for remote shell...", self.log_prefix)

                cli_logger.print("Running `{}` as a test", cf.bold("uptime"))
                while time.time() < deadline and \
                        not self.provider.is_terminated(self.node_id):
                    try:
                        cli_logger.old_debug(
                            logger,
                            "{}Waiting for remote shell...",
                            self.log_prefix)

                        self.cmd_runner.run("uptime")
                        cli_logger.old_debug(
                            logger,
                            "Uptime succeeded.")
                        cli_logger.success("Success.")
                        return True
                    except CLILoggerError as e:
                        # ssh: connect to host 52.27.104.32 port 22: Operation timed out
                        # todo: catch ^

                        if e.type == "ssh_command_failed":
                            cli_logger.print(
                                "SSH still not available, retrying.")

                        time.sleep(READY_CHECK_INTERVAL)
                    except Exception as e:
                        retry_str = str(e)
                        if hasattr(e, "cmd"):
                            retry_str = "(Exit Status {}): {}".format(
                                e.returncode, " ".join(e.cmd))

                        # the way we handle exceptions here is awful
                        cli_logger.print(
                            "SSH still not available ({}), retrying.",
                            retry_str)
                        cli_logger.old_debug(
                            logger,
                            "{}Node not up, retrying: {}",
                            self.log_prefix, retry_str)

                        time.sleep(READY_CHECK_INTERVAL)

        assert False, "Unable to connect to node"

    def do_update(self):
        self.provider.set_node_tags(
            self.node_id, {TAG_RAY_NODE_STATUS: STATUS_WAITING_FOR_SSH})
        cli_logger.keyvalue("New status", STATUS_WAITING_FOR_SSH)

        deadline = time.time() + NODE_START_WAIT_S
        self.wait_ready(deadline)

        node_tags = self.provider.node_tags(self.node_id)
        logger.debug("Node tags: {}".format(str(node_tags)))
        if node_tags.get(TAG_RAY_RUNTIME_CONFIG) == self.runtime_hash:
            # todo: we lie in the confirmation message since
            # full setup might be cancelled here
            cli_logger.print(
                "Configuration already up to date, "
                "skipping file mounts, initalization and setup commands.")
            cli_logger.old_info(
                logger,
                "{}{} already up-to-date, skip to ray start",
                self.log_prefix, self.node_id)
        else:
            cli_logger.print(
                "Updating cluster configuration.",
                _tags=dict(hash=self.runtime_hash))

            self.provider.set_node_tags(
                self.node_id, {TAG_RAY_NODE_STATUS: STATUS_SYNCING_FILES})
            cli_logger.keyvalue("New status", STATUS_SYNCING_FILES)
            self.sync_file_mounts(self.rsync_up)

            # Run init commands
            self.provider.set_node_tags(
                self.node_id, {TAG_RAY_NODE_STATUS: STATUS_SETTING_UP})
            cli_logger.keyvalue("New status", STATUS_SETTING_UP)

            if self.initialization_commands:
                with cli_logger.group(
                    "Running initialization commands",
                    _numbered=("[]", 3, 5)):  # todo: fix command numbering
                    with LogTimer(
                            self.log_prefix + "Initialization commands",
                            show_status=True):

                        for cmd in self.initialization_commands:
                            self.cmd_runner.run(cmd)
            else:
                cli_logger.print(
                    "No initialization commands to run.",
                    _numbered=("[]", 3, 5))

            if self.setup_commands:
                with cli_logger.group(
                    "Running setup commands",
                    _numbered=("[]", 4, 5)): # todo: fix command numbering
                    with LogTimer(
                            self.log_prefix + "Setup commands",
                            show_status=True):

                        for cmd in self.setup_commands:
                            self.cmd_runner.run(cmd)
            else:
                cli_logger.print(
                    "No setup commands to run.",
                    _numbered=("[]", 4, 5))

        with cli_logger.group(
            "Starting the Ray runtime",
            _numbered=("[]", 5, 5)):
            with LogTimer(
                    self.log_prefix + "Ray start commands", show_status=True):
                for cmd in self.ray_start_commands:
                    self.cmd_runner.run(cmd)

    def rsync_up(self, source, target):
        cli_logger.old_info(
            logger,
            "{}Syncing {} to {}...",
            self.log_prefix,
            source, target)

        self.cmd_runner.run_rsync_up(source, target)
        cli_logger.verbose(
            "`rsync`ed {} (local) to {} (remote)",
            cf.bold(source), cf.bold(target))

    def rsync_down(self, source, target):
        cli_logger.old_info(
            logger,
            "{}Syncing {} from {}...",
            self.log_prefix,
            source, target)

        self.cmd_runner.run_rsync_down(source, target)
        cli_logger.verbose(
            "`rsync`ed {} (remote) to {} (local)",
            cf.bold(source), cf.bold(target))


class NodeUpdaterThread(NodeUpdater, Thread):
    def __init__(self, *args, **kwargs):
        Thread.__init__(self)
        NodeUpdater.__init__(self, *args, **kwargs)
        self.exitcode = -1
