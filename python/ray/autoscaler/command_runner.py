from getpass import getuser
from shlex import quote
from typing import List, Tuple
import click
import hashlib
import logging
import os
import subprocess
import sys
import time
import re
import tempfile

from ray.autoscaler.docker import check_docker_running_cmd, with_docker_exec
from ray.autoscaler.log_timer import LogTimer

from ray.autoscaler.cli_logger import cli_logger
import colorful as cf

logger = logging.getLogger(__name__)

# How long to wait for a node to start, in seconds
NODE_START_WAIT_S = 300
HASH_MAX_LENGTH = 10
KUBECTL_RSYNC = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "kubernetes/kubectl-rsync.sh")


class ProcessRunnerError(Exception):
    def __init__(self,
                 msg,
                 msg_type,
                 code=None,
                 command=None,
                 special_case=None):
        super(ProcessRunnerError, self).__init__(
            "{} (discovered={}): type={}, code={}, command={}".format(
                msg, special_case, msg_type, code, command))

        self.msg_type = msg_type
        self.code = code
        self.command = command

        self.special_case = special_case


def _with_interactive(cmd):
    force_interactive = ("true && source ~/.bashrc && "
                         "export OMP_NUM_THREADS=1 PYTHONWARNINGS=ignore && ")
    hide_ttys = "> >(tee) 2> >(tee 1>&2)"
    # todo: this might need to be disabled iff we are dumping output

    return [
        "bash", "--login", "-c", "-i",
        quote(force_interactive + "{ " + cmd + "; }" + hide_ttys)]


class CommandRunnerInterface:
    """Interface to run commands on a remote cluster node.

    Command runner instances are returned by provider.get_command_runner()."""

    def run(self,
            cmd: str = None,
            timeout: int = 120,
            exit_on_fail: bool = False,
            port_forward: List[Tuple[int, int]] = None,
            with_output: bool = False,
            **kwargs) -> str:
        """Run the given command on the cluster node and optionally get output.

        Args:
            cmd (str): The command to run.
            timeout (int): The command timeout in seconds.
            exit_on_fail (bool): Whether to sys exit on failure.
            port_forward (list): List of (local, remote) ports to forward, or
                a single tuple.
            with_output (bool): Whether to return output.
        """
        raise NotImplementedError

    def run_rsync_up(self, source: str, target: str) -> None:
        """Rsync files up to the cluster node.

        Args:
            source (str): The (local) source directory or file.
            target (str): The (remote) destination path.
        """
        raise NotImplementedError

    def run_rsync_down(self, source: str, target: str) -> None:
        """Rsync files down from the cluster node.

        Args:
            source (str): The (remote) source directory or file.
            target (str): The (local) destination path.
        """
        raise NotImplementedError

    def remote_shell_command_str(self) -> str:
        """Return the command the user can use to open a shell."""
        raise NotImplementedError


class KubernetesCommandRunner(CommandRunnerInterface):
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
            with_output=False,
            **kwargs):
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
            final_cmd += _with_interactive(cmd)
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
            "ServerAliveCountMax": 3
        }
        if control_path:
            self.arg_dict.update({
                "ControlMaster": "auto",
                "ControlPath": "{}/%C".format(control_path),
                "ControlPersist": "10s",
            })
        self.arg_dict.update(kwargs)

    def to_ssh_options_list(self, *, timeout=60):
        self.arg_dict["ConnectTimeout"] = "{}s".format(timeout)
        return ["-i", self.ssh_key] + [
            x for y in (["-o", "{}={}".format(k, v)]
                        for k, v in self.arg_dict.items()
                        if v is not None) for x in y
        ]


class SSHCommandRunner(CommandRunnerInterface):
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
        self.ssh_proxy_command = auth_config.get("ssh_proxy_command", None)
        self.ssh_options = SSHOptions(
            self.ssh_private_key,
            self.ssh_control_path,
            ProxyCommand=self.ssh_proxy_command)

        self.known_host_update_msg_re = re.compile(
            r"\s*Warning: Permanently added '.+' \(.+\) "
            r"to the list of known hosts.\s*")
        self.connection_closed_msg_re = re.compile(
            r"\s*Shared connection to .+ closed.\s*")
        self.timeout_msg_re = re.compile(
            r"\s*ssh: connect to host .+ port .+: "
            r"Operation timed out\s*")
        self.conn_refused_msg_re = re.compile(
            r"\s*ssh: connect to host .+ port .+: Connection refused\s*")
        # todo: check for other connection failures for better error messages?

    def _get_node_ip(self):
        if self.use_internal_ip:
            return self.provider.internal_ip(self.node_id)
        else:
            return self.provider.external_ip(self.node_id)

    def wait_for_ip(self, deadline):
        # if we have IP do not print waiting info
        ip = self._get_node_ip()
        if ip is not None:
            cli_logger.labeled_value("Fetched IP", ip)
            return ip

        interval = 10
        with cli_logger.timed("Waiting for IP"):
            while time.time() < deadline and \
                    not self.provider.is_terminated(self.node_id):
                cli_logger.old_info(logger, "{}Waiting for IP...",
                                    self.log_prefix)

                ip = self._get_node_ip()
                if ip is not None:
                    cli_logger.labeled_value("Received", ip)
                    return ip
                cli_logger.print("Not yet available, retrying in {} seconds",
                                 cf.bold(str(interval)))
                time.sleep(interval)

        return None

    def _set_ssh_ip_if_required(self):
        if self.ssh_ip is not None:
            return

        # We assume that this never changes.
        #   I think that's reasonable.
        deadline = time.time() + NODE_START_WAIT_S
        with LogTimer(self.log_prefix + "Got IP"):
            ip = self.wait_for_ip(deadline)

            cli_logger.doassert(ip is not None,
                                "Could not get node IP.")  # todo: msg
            assert ip is not None, "Unable to find IP of node"

        self.ssh_ip = ip

        # This should run before any SSH commands and therefore ensure that
        #   the ControlPath directory exists, allowing SSH to maintain
        #   persistent sessions later on.
        try:
            os.makedirs(self.ssh_control_path, mode=0o700, exist_ok=True)
        except OSError as e:
            cli_logger.warning(e)  # todo: msg
            cli_logger.old_warning(logger, e)

    def _read_subprocess_stream(self, f, output_file,
                                is_stdout=False,
                                stderr_to_file=False):
        """Read and process a subprocess output stream in a loop.

        Ran in a thread each for both `stdout` and `stderr` to
        allow for cross-platform asynchronous IO.

        Note: `select`-based IO would probably be better, but Windows has
        absolutely no support for it, and Linux support varies somewhat.
        Spefically, Older *nix systems might have quirks in how they
        handle `select` on pipes.

        Args:
            f: File object for the stream.
            is_stdout (bool):
                When `is_stdout` is `False`, the stream is assumed to
                be `stderr`. Different error message detectors are used,
                and the output is displayed to the user unless it matches
                a special case (e.g. SSH timeout), in which case this is
                left up to the caller.
        """

        detected_special_case = None
        while True:
            # ! Readline here is crucial.
            # ! Normal `read()` will block until EOF instead of until
            #   something is available.
            l = f.readline()

            if l is None or l == "":
                # EOF
                break

            if is_stdout:
                print('stdout', l)
                output_file.write(l+"\n")
            else:
                print('stderr', l)

                if self.connection_closed_msg_re.\
                    fullmatch(l) is not None:
                    # Do not log "connection closed" messages which SSH
                    # puts in stderr for no reason.
                    #
                    # They are never errors since the connection will
                    # close no matter whether the command succeeds or not.
                    continue

                if self.timeout_msg_re.\
                    fullmatch(l) is not None:
                    # Timeout is not really an error but rather a special
                    # condition. It should be handled by the caller, since
                    # network conditions/nodes in the early stages of boot
                    # are expected to sometimes cause connection timeouts.
                    if detected_special_case is not None:
                        raise ValueError(
                            "Bug: ssh_timeout conflicts with another "
                            "special codition: " + detected_special_case)

                    detected_special_case="ssh_timeout"
                    continue

                if self.conn_refused_msg_re.\
                    fullmatch(l) is not None:
                    # Connection refused is not really an error but
                    # rather a special condition. It should be handled by
                    # the caller, since network conditions/nodes in the
                    # early stages of boot are expected to sometimes cause
                    # CONN_REFUSED.
                    if detected_special_case is not None:
                        raise ValueError(
                            "Bug: ssh_conn_refused conflicts with another "
                            "special codition: " + detected_special_case)

                    detected_special_case="ssh_conn_refused"
                    continue

                if self.known_host_update_msg_re.\
                    fullmatch(l) is not None:
                    # Since we ignore SSH host control anyway
                    # (-o UserKnownHostsFile=/dev/null),
                    # we should silence the host control warnings.
                    continue

                if stderr_to_file:
                    output_file.write(l+"\n")

                cli_logger.error(l)

        return detected_special_case

    def _run_and_process_output(self, cmd, output_file, stderr_to_file=False):
        """Run a command and process its output for special cases.

        Args:
            cmd (List[str]): Command to run.
            output_file:
                File to redirect output to.
                Only redirects stdout by default.
            stderr_to_file (bool):
                If `stderr_to_file` is `True`, stderr will also be written
                to `output_file`.
        """

        with self.process_runner.Popen(
            cmd,
            # Do not inherit stdin as it messes with bash signals
            # (ctrl-C for SIGINT) and these commands aren't supposed to
            # take input anyway.
            stdin=self.process_runner.PIPE,
            stdout=self.process_runner.PIPE,
            stderr=self.process_runner.PIPE,
            bufsize=1, # line buffering
            universal_newlines=True # text mode outputs
        ) as p:
            from concurrent.futures import ThreadPoolExecutor

            # Closing stdin might be necessary to signal EOF to some
            # apps (they might get stuck waiting for input forever otherwise).
            p.stdin.close()

            # We do NOT USE the pooling feature of `ThreadPoolExecutor`.
            # We ONLY rely on it's ability to create futures from threads.
            # This instance will be `shutdown()` ASAP so it's fine to
            # create one in such a weird place.
            #
            # This approach allows us to HAVE NO CUSTOM SYNCHRONIZATION by
            # off-loading the return value and exception passing to the
            # standard library (`ThreadPoolExecutor` internals).
            #
            # The code is thus 100% thread-safe as long as the stream readers
            # are read-only except for return values and possible exceptions.
            with ThreadPoolExecutor(max_workers=2) as pool:
                stdout_future = \
                    pool.submit(self._read_subprocess_stream,
                        p.stdout, output_file,
                        is_stdout=True,
                        stderr_to_file=stderr_to_file)
                stderr_future = \
                    pool.submit(self._read_subprocess_stream,
                        p.stderr, output_file,
                        is_stdout=False,
                        stderr_to_file=stderr_to_file)

                # Regarding command timeout (parameter of `self.run()`):
                # The timeout is passed to SSH by default, so here
                # we just rely on SSH to be well-behaved and exit + close all
                # streams when timed out.
                #
                # We do not manually set a thread timeout here, but it is
                # certainly an option if we ever want to be extra-safe.
                pool.shutdown()  # Wait for completion.

                # Update `p.returncode`
                p.poll()

                detected_special_case = stdout_future.result()
                if stderr_future.result() is not None:
                    if detected_special_case is not None:
                        # This might some day need to be changed.
                        # We should probably make sure the two special cases
                        # are compatible then and that we can handle both by
                        # e.g. reporting both to the caller.
                        raise ValueError(
                            "Bug: found a special case in both stdout and "
                            "stderr this is not valid behavior at the time "
                            "of writing.")
                    detected_special_case = stderr_future.result()

                if p.returncode > 0:
                    # Process failed, but not due to a signal, since signals
                    # set the exit code to a negative value.
                    raise ProcessRunnerError(
                        "Command failed",
                        "ssh_command_failed",
                        code=p.returncode,
                        command=cmd,
                        special_case=detected_special_case)
                elif p.returncode < 0:
                    # Process failed due to a signal, since signals
                    # set the exit code to a negative value.
                    raise ProcessRunnerError(
                        "Command failed",
                        "ssh_command_failed",
                        code=p.returncode,
                        command=cmd,
                        special_case="died_to_signal")

    def _run_redirected(self, cmd):
        """Run a command and optionally redirect output to a file.

        Args:
            cmd (List[str]): Command to run.
        """

        if cli_logger.dump_command_output:
            # todo: stderr should go to sys.stderr probably
            return self._run_and_process_output(cmd,
                                                output_file=sys.stdout,
                                                stderr_to_file=True)
        else:
            tmpfile_path = os.path.join(
                tempfile.gettempdir(),
                "ray-up-{}-{}.txt".format(cmd[0], time.time()))
            with open(tmpfile_path,
                      mode="w",
                      # line buffering
                      buffering=1) as tmp:
                cli_logger.verbose(
                    "Command stdout is redirected to {}",
                    cf.bold(tmp.name))
                cli_logger.verbose(
                    cf.gray("Use -vvv to dump to console instead"))

                return self._run_and_process_output(cmd,
                                                    output_file=tmp,
                                                    stderr_to_file=True)

    def _run_raw(self, final_cmd,
                 with_output=False, exit_on_fail=False):
        """Run a command without pre-processing special options.

        Args:
            cmd (List[str]):
                Full command to run. Should include SSH options and other
                processing that we do.
            with_output (bool):
                If `with_output` is `True`, command stdout and stderr
                will be captured and returned.
            exit_on_fail (bool):
                If `exit_on_fail` is `True`, the process will exit
                if the command fails (exits with a code other than 0).
        """

        try:
            # For now, if the output is needed we just skip the new logic.
            # In the future we could update the new logic to support
            # capturing output, but it is probably not needed.
            if not cli_logger.old_style and not with_output:
                return self._run_redirected(final_cmd)
            if with_output:
                return self.process_runner.check_output(final_cmd)
            else:
                self.process_runner.check_call(final_cmd)
        except subprocess.CalledProcessError as e:
            quoted_cmd = " ".join(final_cmd[:-1] + [quote(final_cmd[-1])])
            if not cli_logger.old_style:
                raise ProcessRunnerError(
                    "Command failed",
                    "ssh_command_failed",
                    code=e.returncode,
                    command=quoted_cmd)

            if exit_on_fail:
                raise click.ClickException(
                    "Command failed: \n\n  {}\n".format(quoted_cmd)) \
                    from None
            else:
                raise click.ClickException(
                    "SSH command Failed. See above for the output from the"
                    " failure.") from None

    def run(self,
            cmd,
            timeout=120,
            exit_on_fail=False,
            port_forward=None,
            with_output=False,
            ssh_options_override=None,
            **kwargs):
        ssh_options = ssh_options_override or self.ssh_options

        assert isinstance(
            ssh_options, SSHOptions
        ), "ssh_options must be of type SSHOptions, got {}".format(
            type(ssh_options))

        self._set_ssh_ip_if_required()

        ssh = ["ssh", "-tt"]

        if port_forward:
            with cli_logger.group("Forwarding ports"):
                if not isinstance(port_forward, list):
                    port_forward = [port_forward]
                for local, remote in port_forward:
                    cli_logger.verbose(
                        "Forwarding port {} to port {} on localhost.",
                        cf.bold(local), cf.bold(remote))  # todo: msg
                    cli_logger.old_info(logger,
                                        "{}Forwarding {} -> localhost:{}",
                                        self.log_prefix, local, remote)
                    ssh += ["-L", "{}:localhost:{}".format(remote, local)]

        final_cmd = ssh + ssh_options.to_ssh_options_list(timeout=timeout) + [
            "{}@{}".format(self.ssh_user, self.ssh_ip)
        ]
        if cmd:
            final_cmd += _with_interactive(cmd)
            cli_logger.old_info(logger, "{}Running {}", self.log_prefix,
                                " ".join(final_cmd))
        else:
            # We do this because `-o ControlMaster` causes the `-N` flag to
            # still create an interactive shell in some ssh versions.
            final_cmd.append(quote("while true; do sleep 86400; done"))

        cli_logger.verbose("Running `{}`", cf.bold(cmd))
        with cli_logger.indented():
            cli_logger.very_verbose("Full command is `{}`",
                                    cf.bold(" ".join(final_cmd)))

        if cli_logger.verbosity > 0:
            with cli_logger.indented():
                self._run_raw(final_cmd, with_output, exit_on_fail)
        else:
            self._run_raw(final_cmd, with_output, exit_on_fail)

    def run_rsync_up(self, source, target):
        self._set_ssh_ip_if_required()
        command = [
            "rsync", "--rsh",
            subprocess.list2cmdline(
                ["ssh"] + self.ssh_options.to_ssh_options_list(timeout=120)),
            "-avz", source, "{}@{}:{}".format(self.ssh_user, self.ssh_ip,
                                              target)
        ]
        cli_logger.verbose("Running `{}`", cf.bold(" ".join(command)))
        self._run_raw(command)

    def run_rsync_down(self, source, target):
        self._set_ssh_ip_if_required()

        command = [
            "rsync", "--rsh",
            subprocess.list2cmdline(
                ["ssh"] + self.ssh_options.to_ssh_options_list(timeout=120)),
            "-avz", "{}@{}:{}".format(self.ssh_user, self.ssh_ip,
                                      source), target
        ]
        cli_logger.verbose("Running `{}`", cf.bold(" ".join(command)))
        self._run_raw(command)

    def remote_shell_command_str(self):
        return "ssh -o IdentitiesOnly=yes -i {} {}@{}\n".format(
            self.ssh_private_key, self.ssh_user, self.ssh_ip)


class DockerCommandRunner(SSHCommandRunner):
    def __init__(self, docker_config, **common_args):
        self.ssh_command_runner = SSHCommandRunner(**common_args)
        self.docker_name = docker_config["container_name"]
        self.docker_config = docker_config
        self.home_dir = None
        self._check_docker_installed()
        self.shutdown = False

    def run(self,
            cmd,
            timeout=120,
            exit_on_fail=False,
            port_forward=None,
            with_output=False,
            run_env=True,
            ssh_options_override=None,
            **kwargs):
        if run_env == "auto":
            run_env = "host" if cmd.find("docker") == 0 else "docker"

        if run_env == "docker":
            cmd = self._docker_expand_user(cmd, any_char=True)
            cmd = with_docker_exec(
                [cmd], container_name=self.docker_name,
                with_interactive=True)[0]

        if self.shutdown:
            cmd += "; sudo shutdown -h now"
        return self.ssh_command_runner.run(
            cmd,
            timeout=timeout,
            exit_on_fail=exit_on_fail,
            port_forward=port_forward,
            with_output=with_output,
            ssh_options_override=ssh_options_override)

    def run_rsync_up(self, source, target):
        self.ssh_command_runner.run_rsync_up(source, target)
        if self._check_container_status():
            self.ssh_command_runner.run("docker cp {} {}:{}".format(
                target, self.docker_name, self._docker_expand_user(target)))

    def run_rsync_down(self, source, target):
        self.ssh_command_runner.run("docker cp {}:{} {}".format(
            self.docker_name, self._docker_expand_user(source), source))
        self.ssh_command_runner.run_rsync_down(source, target)

    def remote_shell_command_str(self):
        inner_str = self.ssh_command_runner.remote_shell_command_str().replace(
            "ssh", "ssh -tt", 1).strip("\n")
        return inner_str + " docker exec -it {} /bin/bash\n".format(
            self.docker_name)

    def _check_docker_installed(self):
        try:
            self.ssh_command_runner.run("command -v docker")
            return
        except Exception:
            install_commands = [
                "curl -fsSL https://get.docker.com -o get-docker.sh",
                "sudo sh get-docker.sh", "sudo usermod -aG docker $USER",
                "sudo systemctl restart docker -f"
            ]
            logger.error(
                "Docker not installed. You can install Docker by adding the "
                "following commands to 'initialization_commands':\n" +
                "\n".join(install_commands))

    def _shutdown_after_next_cmd(self):
        self.shutdown = True

    def _check_container_status(self):
        no_exist = "not_present"
        cmd = check_docker_running_cmd(self.docker_name) + " ".join(
            ["||", "echo", quote(no_exist)])
        output = self.ssh_command_runner.run(
            cmd, with_output=True).decode("utf-8").strip()
        if no_exist in output:
            return False
        return "true" in output.lower()

    def _docker_expand_user(self, string, any_char=False):
        user_pos = string.find("~")
        if user_pos > -1:
            if self.home_dir is None:
                self.home_dir = self.ssh_command_runner.run(
                    "docker exec {} env | grep HOME | cut -d'=' -f2".format(
                        self.docker_name),
                    with_output=True).decode("utf-8").strip()

            if any_char:
                return string.replace("~/", self.home_dir + "/")

            elif not any_char and user_pos == 0:
                return string.replace("~", self.home_dir, 1)

        return string
