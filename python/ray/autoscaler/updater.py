import click
import logging
import os
import subprocess
import time

from threading import Thread

from ray.autoscaler.tags import TAG_RAY_NODE_STATUS, TAG_RAY_RUNTIME_CONFIG, \
    TAG_RAY_FILE_MOUNTS_CONTENTS, \
    STATUS_UP_TO_DATE, STATUS_UPDATE_FAILED, STATUS_WAITING_FOR_SSH, \
    STATUS_SETTING_UP, STATUS_SYNCING_FILES
from ray.autoscaler.command_runner import NODE_START_WAIT_S, \
    ProcessRunnerError
from ray.autoscaler.log_timer import LogTimer

import ray.autoscaler.subprocess_output_util as cmd_output_util

from ray.autoscaler.cli_logger import cli_logger
from ray import ray_constants
import colorful as cf

logger = logging.getLogger(__name__)

READY_CHECK_INTERVAL = 5


class NodeUpdater:
    """A process for syncing files and running init commands on a node.

    Arguments:
        node_id: the Node ID
        provider_config: Provider section of autoscaler yaml
        provider: NodeProvider Class
        auth_config: Auth section of autoscaler yaml
        cluster_name: the name of the cluster.
        file_mounts: Map of remote to local paths
        initialization_commands: Commands run before container launch
        setup_commands: Commands run before ray starts
        ray_start_commands: Commands to start ray
        runtime_hash: Used to check for config changes
        file_mounts_contents_hash: Used to check for changes to file mounts
        is_head_node: Whether to use head start/setup commands
        process_runner: the module to use to run the commands
            in the CommandRunner. E.g., subprocess.
        use_internal_ip: Wwhether the node_id belongs to an internal ip
            or external ip.
        docker_config: Docker section of autoscaler yaml
    """

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
                 file_mounts_contents_hash,
                 is_head_node,
                 node_resources=None,
                 cluster_synced_files=None,
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
        self.node_resources = node_resources
        self.runtime_hash = runtime_hash
        self.file_mounts_contents_hash = file_mounts_contents_hash
        self.cluster_synced_files = cluster_synced_files
        self.auth_config = auth_config
        self.is_head_node = is_head_node
        self.docker_config = docker_config

    def run(self):
        cli_logger.old_info(logger, "{}Updating to {}", self.log_prefix,
                            self.runtime_hash)

        if cmd_output_util.does_allow_interactive(
        ) and cmd_output_util.is_output_redirected():
            # this is most probably a bug since the user has no control
            # over these settings
            msg = ("Output was redirected for an interactive command. "
                   "Either do not pass `--redirect-command-output` "
                   "or also pass in `--use-normal-shells`.")
            cli_logger.abort(msg)
            raise click.ClickException(msg)

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

            cli_logger.old_error(logger, "{}Error executing: {}\n",
                                 self.log_prefix, error_str)

            cli_logger.error("!!!")
            if hasattr(e, "cmd"):
                cli_logger.error(
                    "Setup command `{}` failed with exit code {}. stderr:",
                    cf.bold(e.cmd), e.returncode)
            else:
                cli_logger.verbose_error("{}", str(vars(e)))
                # todo: handle this better somehow?
                cli_logger.error("{}", str(e))
            # todo: print stderr here
            cli_logger.error("!!!")
            cli_logger.newline()

            if isinstance(e, click.ClickException):
                # todo: why do we ignore this here
                return
            raise

        tags_to_set = {
            TAG_RAY_NODE_STATUS: STATUS_UP_TO_DATE,
            TAG_RAY_RUNTIME_CONFIG: self.runtime_hash,
        }
        if self.file_mounts_contents_hash is not None:
            tags_to_set[
                TAG_RAY_FILE_MOUNTS_CONTENTS] = self.file_mounts_contents_hash

        self.provider.set_node_tags(self.node_id, tags_to_set)
        cli_logger.labeled_value("New status", STATUS_UP_TO_DATE)

        self.exitcode = 0

    def sync_file_mounts(self, sync_cmd, step_numbers=(0, 2)):
        # step_numbers is (# of previous steps, total steps)
        previous_steps, total_steps = step_numbers

        nolog_paths = []
        if cli_logger.verbosity == 0:
            nolog_paths = [
                "~/ray_bootstrap_key.pem", "~/ray_bootstrap_config.yaml"
            ]

        def do_sync(remote_path, local_path, allow_non_existing_paths=False):
            if allow_non_existing_paths and not os.path.exists(local_path):
                # Ignore missing source files. In the future we should support
                # the --delete-missing-args command to delete files that have
                # been removed
                return

            assert os.path.exists(local_path), local_path

            if os.path.isdir(local_path):
                if not local_path.endswith("/"):
                    local_path += "/"
                if not remote_path.endswith("/"):
                    remote_path += "/"

            with LogTimer(self.log_prefix +
                          "Synced {} to {}".format(local_path, remote_path)):
                is_docker = (self.docker_config
                             and self.docker_config["container_name"] != "")
                if not is_docker:
                    # The DockerCommandRunner handles this internally.
                    self.cmd_runner.run(
                        "mkdir -p {}".format(os.path.dirname(remote_path)),
                        run_env="host")
                sync_cmd(local_path, remote_path, file_mount=True)

                if remote_path not in nolog_paths:
                    # todo: timed here?
                    cli_logger.print("{} from {}", cf.bold(remote_path),
                                     cf.bold(local_path))

        # Rsync file mounts
        with cli_logger.group(
                "Processing file mounts",
                _numbered=("[]", previous_steps + 1, total_steps)):
            for remote_path, local_path in self.file_mounts.items():
                do_sync(remote_path, local_path)

        if self.cluster_synced_files:
            with cli_logger.group(
                    "Processing worker file mounts",
                    _numbered=("[]", previous_steps + 2, total_steps)):
                for path in self.cluster_synced_files:
                    do_sync(path, path, allow_non_existing_paths=True)
        else:
            cli_logger.print(
                "No worker file mounts to sync",
                _numbered=("[]", previous_steps + 2, total_steps))

    def wait_ready(self, deadline):
        with cli_logger.group(
                "Waiting for SSH to become available", _numbered=("[]", 1, 6)):
            with LogTimer(self.log_prefix + "Got remote shell"):
                cli_logger.old_info(logger, "{}Waiting for remote shell...",
                                    self.log_prefix)

                cli_logger.print("Running `{}` as a test.", cf.bold("uptime"))
                first_conn_refused_time = None
                while time.time() < deadline and \
                        not self.provider.is_terminated(self.node_id):
                    try:
                        cli_logger.old_debug(logger,
                                             "{}Waiting for remote shell...",
                                             self.log_prefix)

                        # Run outside of the container
                        self.cmd_runner.run("uptime", run_env="host")
                        cli_logger.old_debug(logger, "Uptime succeeded.")
                        cli_logger.success("Success.")
                        return True
                    except ProcessRunnerError as e:
                        first_conn_refused_time = \
                            cmd_output_util.handle_ssh_fails(
                                e, first_conn_refused_time,
                                retry_interval=READY_CHECK_INTERVAL)
                        time.sleep(READY_CHECK_INTERVAL)
                    except Exception as e:
                        # TODO(maximsmol): we should not be ignoring
                        # exceptions if they get filtered properly
                        # (new style log + non-interactive shells)
                        #
                        # however threading this configuration state
                        # is a pain and I'm leaving it for later

                        retry_str = str(e)
                        if hasattr(e, "cmd"):
                            retry_str = "(Exit Status {}): {}".format(
                                e.returncode, " ".join(e.cmd))

                        cli_logger.print(
                            "SSH still not available {}, "
                            "retrying in {} seconds.", cf.dimmed(retry_str),
                            cf.bold(str(READY_CHECK_INTERVAL)))
                        cli_logger.old_debug(logger,
                                             "{}Node not up, retrying: {}",
                                             self.log_prefix, retry_str)

                        time.sleep(READY_CHECK_INTERVAL)

        assert False, "Unable to connect to node"

    def do_update(self):
        self.provider.set_node_tags(
            self.node_id, {TAG_RAY_NODE_STATUS: STATUS_WAITING_FOR_SSH})
        cli_logger.labeled_value("New status", STATUS_WAITING_FOR_SSH)

        deadline = time.time() + NODE_START_WAIT_S
        self.wait_ready(deadline)

        node_tags = self.provider.node_tags(self.node_id)
        logger.debug("Node tags: {}".format(str(node_tags)))

        # runtime_hash will only change whenever the user restarts
        # or updates their cluster with `get_or_create_head_node`
        if node_tags.get(TAG_RAY_RUNTIME_CONFIG) == self.runtime_hash and (
                self.file_mounts_contents_hash is None
                or node_tags.get(TAG_RAY_FILE_MOUNTS_CONTENTS) ==
                self.file_mounts_contents_hash):
            # todo: we lie in the confirmation message since
            # full setup might be cancelled here
            cli_logger.print(
                "Configuration already up to date, "
                "skipping file mounts, initalization and setup commands.",
                _numbered=("[]", "2-5", 6))
            cli_logger.old_info(logger,
                                "{}{} already up-to-date, skip to ray start",
                                self.log_prefix, self.node_id)

            # When resuming from a stopped instance the runtime_hash may be the
            # same, but the container will not be started.
            self.cmd_runner.run_init(
                as_head=self.is_head_node, file_mounts=self.file_mounts)

        else:
            cli_logger.print(
                "Updating cluster configuration.",
                _tags=dict(hash=self.runtime_hash))

            self.provider.set_node_tags(
                self.node_id, {TAG_RAY_NODE_STATUS: STATUS_SYNCING_FILES})
            cli_logger.labeled_value("New status", STATUS_SYNCING_FILES)
            self.sync_file_mounts(self.rsync_up, step_numbers=(2, 6))

            # Only run setup commands if runtime_hash has changed because
            # we don't want to run setup_commands every time the head node
            # file_mounts folders have changed.
            if node_tags.get(TAG_RAY_RUNTIME_CONFIG) != self.runtime_hash:
                # Run init commands
                self.provider.set_node_tags(
                    self.node_id, {TAG_RAY_NODE_STATUS: STATUS_SETTING_UP})
                cli_logger.labeled_value("New status", STATUS_SETTING_UP)

                if self.initialization_commands:
                    with cli_logger.group(
                            "Running initialization commands",
                            _numbered=("[]", 3, 5)):
                        with LogTimer(
                                self.log_prefix + "Initialization commands",
                                show_status=True):
                            for cmd in self.initialization_commands:
                                try:
                                    # Overriding the existing SSHOptions class
                                    # with a new SSHOptions class that uses
                                    # this ssh_private_key as its only __init__
                                    # argument.
                                    # Run outside docker.
                                    self.cmd_runner.run(
                                        cmd,
                                        ssh_options_override_ssh_key=self.
                                        auth_config.get("ssh_private_key"),
                                        run_env="host")
                                except ProcessRunnerError as e:
                                    if e.msg_type == "ssh_command_failed":
                                        cli_logger.error("Failed.")
                                        cli_logger.error(
                                            "See above for stderr.")

                                    raise click.ClickException(
                                        "Initialization command failed."
                                    ) from None
                else:
                    cli_logger.print(
                        "No initialization commands to run.",
                        _numbered=("[]", 3, 6))
                self.cmd_runner.run_init(
                    as_head=self.is_head_node, file_mounts=self.file_mounts)
                if self.setup_commands:
                    with cli_logger.group(
                            "Running setup commands",
                            # todo: fix command numbering
                            _numbered=("[]", 4, 6)):
                        with LogTimer(
                                self.log_prefix + "Setup commands",
                                show_status=True):

                            total = len(self.setup_commands)
                            for i, cmd in enumerate(self.setup_commands):
                                if cli_logger.verbosity == 0 and len(cmd) > 30:
                                    cmd_to_print = cf.bold(cmd[:30]) + "..."
                                else:
                                    cmd_to_print = cf.bold(cmd)

                                cli_logger.print(
                                    "{}",
                                    cmd_to_print,
                                    _numbered=("()", i, total))

                                try:
                                    # Runs in the container if docker is in use
                                    self.cmd_runner.run(cmd, run_env="auto")
                                except ProcessRunnerError as e:
                                    if e.msg_type == "ssh_command_failed":
                                        cli_logger.error("Failed.")
                                        cli_logger.error(
                                            "See above for stderr.")

                                    raise click.ClickException(
                                        "Setup command failed.")
                else:
                    cli_logger.print(
                        "No setup commands to run.", _numbered=("[]", 4, 6))

        with cli_logger.group(
                "Starting the Ray runtime", _numbered=("[]", 6, 6)):
            with LogTimer(
                    self.log_prefix + "Ray start commands", show_status=True):
                for cmd in self.ray_start_commands:
                    if self.node_resources:
                        env_vars = {
                            ray_constants.RESOURCES_ENVIRONMENT_VARIABLE: self.
                            node_resources
                        }
                    else:
                        env_vars = {}
                    try:
                        old_redirected = cmd_output_util.is_output_redirected()
                        cmd_output_util.set_output_redirected(False)
                        # Runs in the container if docker is in use
                        self.cmd_runner.run(
                            cmd,
                            environment_variables=env_vars,
                            run_env="auto")
                        cmd_output_util.set_output_redirected(old_redirected)
                    except ProcessRunnerError as e:
                        if e.msg_type == "ssh_command_failed":
                            cli_logger.error("Failed.")
                            cli_logger.error("See above for stderr.")

                        raise click.ClickException("Start command failed.")

    def rsync_up(self, source, target, file_mount=False):
        cli_logger.old_info(logger, "{}Syncing {} to {}...", self.log_prefix,
                            source, target)

        options = {}
        options["file_mount"] = file_mount
        self.cmd_runner.run_rsync_up(source, target, options=options)
        cli_logger.verbose("`rsync`ed {} (local) to {} (remote)",
                           cf.bold(source), cf.bold(target))

    def rsync_down(self, source, target, file_mount=False):
        cli_logger.old_info(logger, "{}Syncing {} from {}...", self.log_prefix,
                            source, target)

        options = {}
        options["file_mount"] = file_mount
        self.cmd_runner.run_rsync_down(source, target, options=options)
        cli_logger.verbose("`rsync`ed {} (remote) to {} (local)",
                           cf.bold(source), cf.bold(target))


class NodeUpdaterThread(NodeUpdater, Thread):
    def __init__(self, *args, **kwargs):
        Thread.__init__(self)
        NodeUpdater.__init__(self, *args, **kwargs)
        self.exitcode = -1
