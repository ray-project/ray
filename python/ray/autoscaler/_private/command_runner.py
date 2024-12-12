import hashlib
import json
import logging
import os
import subprocess
import sys
import time
from getpass import getuser
from shlex import quote
from typing import Dict, List

import click

from ray._private.ray_constants import DEFAULT_OBJECT_STORE_MAX_MEMORY_BYTES
from ray.autoscaler._private.cli_logger import cf, cli_logger
from ray.autoscaler._private.constants import (
    AUTOSCALER_NODE_SSH_INTERVAL_S,
    AUTOSCALER_NODE_START_WAIT_S,
    DEFAULT_OBJECT_STORE_MEMORY_PROPORTION,
)
from ray.autoscaler._private.log_timer import LogTimer
from ray.autoscaler._private.subprocess_output_util import (
    ProcessRunnerError,
    is_output_redirected,
    run_cmd_redirected,
)
from ray.autoscaler.command_runner import CommandRunnerInterface

logger = logging.getLogger(__name__)

# How long to wait for a node to start, in seconds
HASH_MAX_LENGTH = 10
KUBECTL_RSYNC = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "_kubernetes/kubectl-rsync.sh"
)
MAX_HOME_RETRIES = 3
HOME_RETRY_DELAY_S = 5

_config = {"use_login_shells": True, "silent_rsync": True}


def is_rsync_silent():
    return _config["silent_rsync"]


def set_rsync_silent(val):
    """Choose whether to silence rsync output.

    Most commands will want to list rsync'd files themselves rather than
    print the default rsync spew.
    """
    _config["silent_rsync"] = val


def is_using_login_shells():
    return _config["use_login_shells"]


def set_using_login_shells(val: bool):
    """Choose between login and non-interactive shells.

    Non-interactive shells have the benefit of receiving less output from
    subcommands (since progress bars and TTY control codes are not printed).
    Sometimes this can be significant since e.g. `pip install` prints
    hundreds of progress bar lines when downloading.

    Login shells have the benefit of working very close to how a proper bash
    session does, regarding how scripts execute and how the environment is
    setup. This is also how all commands were ran in the past. The only reason
    to use login shells over non-interactive shells is if you need some weird
    and non-robust tool to work.

    Args:
        val: If true, login shells will be used to run all commands.
    """
    _config["use_login_shells"] = val


def _with_environment_variables(cmd: str, environment_variables: Dict[str, object]):
    """Prepend environment variables to a shell command.

    Args:
        cmd: The base command.
        environment_variables (Dict[str, object]): The set of environment
            variables. If an environment variable value is a dict, it will
            automatically be converted to a one line yaml string.
    """

    as_strings = []
    for key, val in environment_variables.items():
        val = json.dumps(val, separators=(",", ":"))
        s = "export {}={};".format(key, quote(val))
        as_strings.append(s)
    all_vars = "".join(as_strings)
    return all_vars + cmd


def _with_interactive(cmd):
    force_interactive = (
        f"source ~/.bashrc; "
        f"export OMP_NUM_THREADS=1 PYTHONWARNINGS=ignore && ({cmd})"
    )
    return ["bash", "--login", "-c", "-i", quote(force_interactive)]


class SSHOptions:
    def __init__(self, ssh_key, control_path=None, **kwargs):
        self.ssh_key = ssh_key
        self.arg_dict = {
            # Supresses initial fingerprint verification.
            "StrictHostKeyChecking": "no",
            # SSH IP and fingerprint pairs no longer added to known_hosts.
            # This is to remove a "REMOTE HOST IDENTIFICATION HAS CHANGED"
            # warning if a new node has the same IP as a previously
            # deleted node, because the fingerprints will not match in
            # that case.
            "UserKnownHostsFile": os.devnull,
            # Try fewer extraneous key pairs.
            "IdentitiesOnly": "yes",
            # Abort if port forwarding fails (instead of just printing to
            # stderr).
            "ExitOnForwardFailure": "yes",
            # Quickly kill the connection if network connection breaks (as
            # opposed to hanging/blocking).
            "ServerAliveInterval": 5,
            "ServerAliveCountMax": 3,
        }
        if control_path:
            self.arg_dict.update(
                {
                    "ControlMaster": "auto",
                    "ControlPath": "{}/%C".format(control_path),
                    "ControlPersist": "10s",
                }
            )
        self.arg_dict.update(kwargs)

    def to_ssh_options_list(self, *, timeout=60):
        self.arg_dict["ConnectTimeout"] = "{}s".format(timeout)
        ssh_key_option = ["-i", self.ssh_key] if self.ssh_key else []
        return ssh_key_option + [
            x
            for y in (
                ["-o", "{}={}".format(k, v)]
                for k, v in self.arg_dict.items()
                if v is not None
            )
            for x in y
        ]


class SSHCommandRunner(CommandRunnerInterface):
    def __init__(
        self,
        log_prefix,
        node_id,
        provider,
        auth_config,
        cluster_name,
        process_runner,
        use_internal_ip,
    ):

        ssh_control_hash = hashlib.sha1(cluster_name.encode()).hexdigest()
        ssh_user_hash = hashlib.sha1(getuser().encode()).hexdigest()
        ssh_control_path = "/tmp/ray_ssh_{}/{}".format(
            ssh_user_hash[:HASH_MAX_LENGTH], ssh_control_hash[:HASH_MAX_LENGTH]
        )

        self.cluster_name = cluster_name
        self.log_prefix = log_prefix
        self.process_runner = process_runner
        self.node_id = node_id
        self.use_internal_ip = use_internal_ip
        self.provider = provider
        self.ssh_private_key = auth_config.get("ssh_private_key")
        self.ssh_user = auth_config["ssh_user"]
        self.ssh_control_path = ssh_control_path
        self.ssh_ip = None
        self.ssh_proxy_command = auth_config.get("ssh_proxy_command", None)
        self.ssh_options = SSHOptions(
            self.ssh_private_key,
            self.ssh_control_path,
            ProxyCommand=self.ssh_proxy_command,
        )

    def _get_node_ip(self):
        if self.use_internal_ip:
            return self.provider.internal_ip(self.node_id)
        else:
            return self.provider.external_ip(self.node_id)

    def _wait_for_ip(self, deadline):
        # if we have IP do not print waiting info
        ip = self._get_node_ip()
        if ip is not None:
            cli_logger.labeled_value("Fetched IP", ip)
            return ip

        interval = AUTOSCALER_NODE_SSH_INTERVAL_S
        with cli_logger.group("Waiting for IP"):
            while time.time() < deadline and not self.provider.is_terminated(
                self.node_id
            ):
                ip = self._get_node_ip()
                if ip is not None:
                    cli_logger.labeled_value("Received", ip)
                    return ip
                cli_logger.print(
                    "Not yet available, retrying in {} seconds", cf.bold(str(interval))
                )
                time.sleep(interval)

        return None

    def _set_ssh_ip_if_required(self):
        if self.ssh_ip is not None:
            return

        # We assume that this never changes.
        #   I think that's reasonable.
        deadline = time.time() + AUTOSCALER_NODE_START_WAIT_S
        with LogTimer(self.log_prefix + "Got IP"):
            ip = self._wait_for_ip(deadline)

            cli_logger.doassert(ip is not None, "Could not get node IP.")  # todo: msg
            assert ip is not None, "Unable to find IP of node"

        self.ssh_ip = ip

        # This should run before any SSH commands and therefore ensure that
        #   the ControlPath directory exists, allowing SSH to maintain
        #   persistent sessions later on.
        try:
            os.makedirs(self.ssh_control_path, mode=0o700, exist_ok=True)
        except OSError as e:
            cli_logger.warning("{}", str(e))  # todo: msg

    def _run_helper(
        self, final_cmd, with_output=False, exit_on_fail=False, silent=False
    ):
        """Run a command that was already setup with SSH and `bash` settings.

        Args:
            cmd (List[str]):
                Full command to run. Should include SSH options and other
                processing that we do.
            with_output (bool):
                If `with_output` is `True`, command stdout will be captured and
                returned.
            exit_on_fail (bool):
                If `exit_on_fail` is `True`, the process will exit
                if the command fails (exits with a code other than 0).

        Raises:
            ProcessRunnerError if using new log style and disabled
                login shells.
            click.ClickException if using login shells.
        """
        try:
            # For now, if the output is needed we just skip the new logic.
            # In the future we could update the new logic to support
            # capturing output, but it is probably not needed.
            if not with_output:
                return run_cmd_redirected(
                    final_cmd,
                    process_runner=self.process_runner,
                    silent=silent,
                    use_login_shells=is_using_login_shells(),
                )
            else:
                return self.process_runner.check_output(final_cmd)
        except subprocess.CalledProcessError as e:
            joined_cmd = " ".join(final_cmd)
            if not is_using_login_shells():
                raise ProcessRunnerError(
                    "Command failed",
                    "ssh_command_failed",
                    code=e.returncode,
                    command=joined_cmd,
                )

            if exit_on_fail:
                raise click.ClickException(
                    "Command failed:\n\n  {}\n".format(joined_cmd)
                ) from None
            else:
                fail_msg = "SSH command failed."
                if is_output_redirected():
                    fail_msg += " See above for the output from the failure."
                raise click.ClickException(fail_msg) from None
        finally:
            # Do our best to flush output to terminal.
            # See https://github.com/ray-project/ray/pull/19473.
            sys.stdout.flush()
            sys.stderr.flush()

    def run(
        self,
        cmd,
        timeout=120,
        exit_on_fail=False,
        port_forward=None,
        with_output=False,
        environment_variables: Dict[str, object] = None,
        run_env="auto",  # Unused argument.
        ssh_options_override_ssh_key="",
        shutdown_after_run=False,
        silent=False,
    ):
        if shutdown_after_run:
            cmd += "; sudo shutdown -h now"

        if ssh_options_override_ssh_key:
            if self.ssh_proxy_command:
                ssh_options = SSHOptions(
                    ssh_options_override_ssh_key, ProxyCommand=self.ssh_proxy_command
                )
            else:
                ssh_options = SSHOptions(ssh_options_override_ssh_key)
        else:
            ssh_options = self.ssh_options

        assert isinstance(
            ssh_options, SSHOptions
        ), "ssh_options must be of type SSHOptions, got {}".format(type(ssh_options))

        self._set_ssh_ip_if_required()

        if is_using_login_shells():
            ssh = ["ssh", "-tt"]
        else:
            ssh = ["ssh"]

        if port_forward:
            with cli_logger.group("Forwarding ports"):
                if not isinstance(port_forward, list):
                    port_forward = [port_forward]
                for local, remote in port_forward:
                    cli_logger.verbose(
                        "Forwarding port {} to port {} on localhost.",
                        cf.bold(local),
                        cf.bold(remote),
                    )  # todo: msg
                    ssh += ["-L", "{}:localhost:{}".format(local, remote)]

        final_cmd = (
            ssh
            + ssh_options.to_ssh_options_list(timeout=timeout)
            + ["{}@{}".format(self.ssh_user, self.ssh_ip)]
        )
        if cmd:
            if environment_variables:
                cmd = _with_environment_variables(cmd, environment_variables)
            if is_using_login_shells():
                final_cmd += _with_interactive(cmd)
            else:
                final_cmd += [cmd]
        else:
            # We do this because `-o ControlMaster` causes the `-N` flag to
            # still create an interactive shell in some ssh versions.
            final_cmd.append("while true; do sleep 86400; done")

        cli_logger.verbose("Running `{}`", cf.bold(cmd))
        with cli_logger.indented():
            cli_logger.very_verbose(
                "Full command is `{}`", cf.bold(" ".join(final_cmd))
            )

        if cli_logger.verbosity > 0:
            with cli_logger.indented():
                return self._run_helper(
                    final_cmd, with_output, exit_on_fail, silent=silent
                )
        else:
            return self._run_helper(final_cmd, with_output, exit_on_fail, silent=silent)

    def _create_rsync_filter_args(self, options):
        rsync_excludes = options.get("rsync_exclude") or []
        rsync_filters = options.get("rsync_filter") or []

        exclude_args = [
            ["--exclude", rsync_exclude] for rsync_exclude in rsync_excludes
        ]
        filter_args = [
            ["--filter", "dir-merge,- {}".format(rsync_filter)]
            for rsync_filter in rsync_filters
        ]

        # Combine and flatten the two lists
        return [arg for args_list in exclude_args + filter_args for arg in args_list]

    def run_rsync_up(self, source, target, options=None):
        self._set_ssh_ip_if_required()
        options = options or {}

        command = ["rsync"]
        command += [
            "--rsh",
            subprocess.list2cmdline(
                ["ssh"] + self.ssh_options.to_ssh_options_list(timeout=120)
            ),
        ]
        command += ["-avz"]
        command += self._create_rsync_filter_args(options=options)
        command += [source, "{}@{}:{}".format(self.ssh_user, self.ssh_ip, target)]
        cli_logger.verbose("Running `{}`", cf.bold(" ".join(command)))
        self._run_helper(command, silent=is_rsync_silent())

    def run_rsync_down(self, source, target, options=None):
        self._set_ssh_ip_if_required()

        command = ["rsync"]
        command += [
            "--rsh",
            subprocess.list2cmdline(
                ["ssh"] + self.ssh_options.to_ssh_options_list(timeout=120)
            ),
        ]
        command += ["-avz"]
        command += self._create_rsync_filter_args(options=options)
        command += ["{}@{}:{}".format(self.ssh_user, self.ssh_ip, source), target]
        cli_logger.verbose("Running `{}`", cf.bold(" ".join(command)))
        self._run_helper(command, silent=is_rsync_silent())

    def remote_shell_command_str(self):
        if self.ssh_private_key:
            return "ssh -o IdentitiesOnly=yes -i {} {}@{}\n".format(
                self.ssh_private_key, self.ssh_user, self.ssh_ip
            )
        else:
            return "ssh -o IdentitiesOnly=yes {}@{}\n".format(
                self.ssh_user, self.ssh_ip
            )
