"""Create an AWS instance to reproduce Buildkite CI builds.

This script will take a Buildkite build URL as an argument and create
an AWS instance with the same properties running the same Docker container
as the original Buildkite runner. The user is then attached to this instance
and can reproduce any builds commands as if they were executed within the
runner.

This utility can be used to reproduce and debug build failures that come up
on the Bildkite runner instances but not on a local machine.

Optionally, build commands can be executed automatically. Filters can be added
to exclude some of these commands. For instance, some users may want to execute
all build commands except for the `bazel build` commands, which they would
like to execute manually.

Usage:

    python repro-ci.py [-n instance-name] [-c] [-f filter1] [-f filter2] ...

Arguments:
    -n: Instance name to be used. If an instance with this name already exists,
        it will be reused.
    -c: Execute commands after setting up the machine.
    -f: Filter these commands (do not execute commands that match this
        regex pattern).

"""
import base64
import json
import logging
import os
import random
import re
import shlex
import subprocess
import threading
import time
from numbers import Number
from typing import Any, Dict, List, Optional, Callable

import boto3
import click
import paramiko
import yaml
from pybuildkite.buildkite import Buildkite


def maybe_fetch_buildkite_token():
    if os.environ.get("BUILDKITE_TOKEN", None) is None:
        print("Missing BUILDKITE_TOKEN, retrieving from AWS secrets store")
        os.environ["BUILDKITE_TOKEN"] = boto3.client(
            "secretsmanager", region_name="us-west-2"
        ).get_secret_value(
            SecretId="arn:aws:secretsmanager:us-west-2:029272617770:secret:"
            "buildkite/ro-token")["SecretString"]


def escape(v: Any):
    if isinstance(v, bool):
        return f"{int(v)}"
    elif isinstance(v, Number):
        return str(v)
    elif isinstance(v, list):
        return " ".join(shlex.quote(w) for w in v)
    else:
        return v


def env_str(env: Dict[str, Any]):
    kvs = []
    for k, v in env.items():
        if isinstance(v, bool):
            kvs.append((k, int(v)))
        elif isinstance(v, Number):
            kvs.append((k, str(v)))
        elif isinstance(v, list):
            for i, w in enumerate(v):
                kvs.append((f"{k}_{i}", w))
        else:
            kvs.append((k, v))

    return " ".join(f"{k}={shlex.quote(v)}" for k, v in kvs)


def script_str(v: Any):
    if isinstance(v, bool):
        return f"\"{int(v)}\""
    elif isinstance(v, Number):
        return f"\"{v}\""
    elif isinstance(v, list):
        return "(" + " ".join(f"\"{shlex.quote(w)}\"" for w in v) + ")"
    else:
        return f"\"{shlex.quote(v)}\""


class ReproSession:
    plugin_default_env = {
        "docker": {
            "BUILDKITE_PLUGIN_DOCKER_MOUNT_BUILDKITE_AGENT": False
        }
    }

    def __init__(self,
                 buildkite_token: str,
                 instance_name: Optional[str] = None,
                 logger: Optional[logging.Logger] = None):
        self.logger = logger or logging.getLogger(self.__class__.__name__)

        self.bk = Buildkite()
        self.bk.set_access_token(buildkite_token)

        self.ssh_user = "ec2-user"
        self.ssh_key_name = "buildkite-repro-env"
        self.ssh_key_file = "~/.ssh/buildkite-repro-env.pem"

        self.ec2_client = boto3.client("ec2", region_name="us-west-2")
        self.ec2_resource = boto3.resource("ec2", region_name="us-west-2")

        self.org = None
        self.pipeline = None
        self.build_id = None
        self.job_id = None

        self.env: Dict[str, str] = {}

        self.aws_instance_name = instance_name
        self.aws_instance_id = None
        self.aws_instance_ip = None

        self.ssh = None

        self.plugins = {}

        self.skipped_commands = []

    def set_session(self, session_url: str):
        # E.g.:
        # https://buildkite.com/ray-project/ray-builders-pr/
        # builds/19635#55a0d71a-831e-4f68-b668-2b10c6f65ee6
        pattern = re.compile(
            "https://buildkite.com/([^/]+)/([^/]+)/builds/([0-9]+)#(.+)")
        org, pipeline, build_id, job_id = pattern.match(session_url).groups()

        self.logger.debug(f"Parsed session URL: {session_url}. "
                          f"Got org='{org}', pipeline='{pipeline}', "
                          f"build_id='{build_id}', job_id='{job_id}'.")

        self.org = org
        self.pipeline = pipeline
        self.build_id = build_id
        self.job_id = job_id

    def fetch_env_variables(self, overwrite: Optional[Dict[str, Any]] = None):
        assert self.bk

        self.env = self.bk.jobs().get_job_environment_variables(
            self.org, self.pipeline, self.build_id, self.job_id)["env"]

        if overwrite:
            self.env.update(overwrite)

        return self.env

    def aws_start_instance(self):
        assert self.env

        if not self.aws_instance_name:
            self.aws_instance_name = (
                f"repro_ci_{self.build_id}_{self.job_id[:8]}")
            self.logger.info(
                f"No instance name provided, using {self.aws_instance_name}")

        instance_type = self.env["BUILDKITE_AGENT_META_DATA_AWS_INSTANCE_TYPE"]
        instance_ami = self.env["BUILDKITE_AGENT_META_DATA_AWS_AMI_ID"]
        instance_sg = "sg-0ccfca2ef191c04ae"
        instance_block_device_mappings = [{
            "DeviceName": "/dev/xvda",
            "Ebs": {
                "VolumeSize": 500
            }
        }]

        # Check if instance exists:
        running_instances = self.ec2_resource.instances.filter(Filters=[{
            "Name": "tag:repro_name",
            "Values": [self.aws_instance_name]
        }, {
            "Name": "instance-state-name",
            "Values": ["running"]
        }])

        self.logger.info(
            f"Check if instance with name {self.aws_instance_name} "
            f"already exists...")

        for instance in running_instances:
            self.aws_instance_id = instance.id
            self.aws_instance_ip = instance.public_ip_address
            self.logger.info(f"Found running instance {self.aws_instance_id}.")
            return

        self.logger.info(
            f"Instance with name {self.aws_instance_name} not found, "
            f"creating...")

        # Else, not running, yet, start.
        instance = self.ec2_resource.create_instances(
            BlockDeviceMappings=instance_block_device_mappings,
            ImageId=instance_ami,
            InstanceType=instance_type,
            KeyName=self.ssh_key_name,
            SecurityGroupIds=[instance_sg],
            TagSpecifications=[{
                "ResourceType": "instance",
                "Tags": [{
                    "Key": "repro_name",
                    "Value": self.aws_instance_name
                }]
            }],
            MinCount=1,
            MaxCount=1,
        )[0]

        self.aws_instance_id = instance.id
        self.logger.info(
            f"Created new instance with ID {self.aws_instance_id}")

    def aws_wait_for_instance(self):
        assert self.aws_instance_id

        self.logger.info("Waiting for instance to come up...")

        repro_instance_state = None
        while repro_instance_state != "running":
            detail = self.ec2_client.describe_instances(
                InstanceIds=[self.aws_instance_id], )
            repro_instance_state = \
                detail["Reservations"][0]["Instances"][0]["State"]["Name"]

            if repro_instance_state != "running":
                time.sleep(2)

        self.aws_instance_ip = detail["Reservations"][0]["Instances"][0][
            "PublicIpAddress"]

    def aws_stop_instance(self):
        assert self.aws_instance_id

        self.ec2_client.terminate_instances(
            InstanceIds=[self.aws_instance_id], )

    def print_stop_command(self):
        click.secho("To stop this instance in the future, run this: ")
        click.secho(
            f"aws ec2 terminate-instances "
            f"--instance-ids={self.aws_instance_id}",
            bold=True)

    def create_new_ssh_client(self):
        assert self.aws_instance_ip

        if self.ssh:
            self.ssh.close()

        self.logger.info(
            "Creating SSH client and waiting for SSH to become available...")

        ssh = paramiko.client.SSHClient()
        ssh.load_system_host_keys()
        ssh.set_missing_host_key_policy(paramiko.WarningPolicy())
        timeout = time.monotonic() + 60
        while time.monotonic() < timeout:
            try:
                ssh.connect(
                    self.aws_instance_ip,
                    username=self.ssh_user,
                    key_filename=os.path.expanduser(self.ssh_key_file))
                break
            except paramiko.ssh_exception.NoValidConnectionsError:
                self.logger.info("SSH not ready, yet, sleeping 5 seconds")
                time.sleep(5)

        self.ssh = ssh
        return self.ssh

    def close_ssh(self):
        self.ssh.close()

    def ssh_exec(self, command, quiet: bool = False, *args, **kwargs):
        result = {}

        def exec():
            stdin, stdout, stderr = self.ssh.exec_command(
                command, get_pty=True)

            output = ""
            for line in stdout.readlines():
                output += line
                if not quiet:
                    print(line, end="")

            for line in stderr.readlines():
                if not quiet:
                    print(line, end="")

            result["output"] = output

        thread = threading.Thread(target=exec)
        thread.start()

        status = time.monotonic() + 30

        while thread.is_alive():
            thread.join(2)
            if time.monotonic() >= status and thread.is_alive():
                self.logger.info("Still executing...")
                status = time.monotonic() + 30

        thread.join()
        return result.get("output", "")

    def execute_ssh_command(
            self,
            command: str,
            env: Optional[Dict[str, str]] = None,
            as_script: bool = False,
            quiet: bool = False,
            command_wrapper: Optional[Callable[[str], str]] = None) -> str:
        assert self.ssh

        if not command_wrapper:

            def command_wrapper(s):
                return s

        full_env = self.env.copy()
        if env:
            full_env.update(env)

        if as_script:
            ftp = self.ssh.open_sftp()
            file = ftp.file("/tmp/script.sh", "w", -1)
            file.write("#!/bin/bash\n")
            for k, v in env.items():
                file.write(f"{k}={script_str(v)}\n")
            file.write(command + "\n")
            file.flush()
            ftp.close()

            full_command = "bash /tmp/script.sh"
        else:
            full_command = f"export {env_str(full_env)}; {command}"

        full_command = command_wrapper(full_command)

        self.logger.debug(f"Executing command: {command}")

        output = self.ssh_exec(full_command, quiet=quiet, get_pty=True)

        return output

    def execute_ssh_commands(self,
                             commands: List[str],
                             env: Optional[Dict[str, str]] = None,
                             quiet: bool = False):
        for command in commands:
            self.execute_ssh_command(command, env=env, quiet=quiet)

    def execute_docker_command(self,
                               command: str,
                               env: Optional[Dict[str, str]] = None,
                               quiet: bool = False):
        def command_wrapper(s):
            escaped = s.replace("'", "'\"'\"'")
            return f"docker exec -it ray_container /bin/bash -ci '{escaped}'"

        self.execute_ssh_command(
            command, env=env, quiet=quiet, command_wrapper=command_wrapper)

    def prepare_instance(self):
        self.create_new_ssh_client()
        output = self.execute_ssh_command("docker ps", quiet=True)
        if "CONTAINER ID" in output:
            self.logger.info("Instance already prepared.")
            return

        self.logger.info("Preparing instance (installing docker etc.)")
        commands = [
            "sudo yum install -y docker", "sudo service docker start",
            f"sudo usermod -aG docker {self.ssh_user}"
        ]
        self.execute_ssh_commands(commands, quiet=True)
        self.create_new_ssh_client()
        self.execute_ssh_command("docker ps", quiet=True)
        self.docker_login()

    def docker_login(self):
        self.logger.info("Logging into docker...")
        credentials = boto3.client(
            "ecr", region_name="us-west-2").get_authorization_token()
        token = base64.b64decode(credentials["authorizationData"][0][
            "authorizationToken"]).decode("utf-8").replace("AWS:", "")
        endpoint = credentials["authorizationData"][0]["proxyEndpoint"]

        self.execute_ssh_command(
            f"docker login -u AWS -p {token} {endpoint}", quiet=True)

    def fetch_buildkite_plugins(self):
        assert self.env

        self.logger.info("Fetching Buildkite plugins")

        plugins = json.loads(self.env["BUILDKITE_PLUGINS"])
        for collection in plugins:
            for plugin, options in collection.items():
                plugin_url, plugin_version = plugin.split("#")
                if not plugin_url.startswith(
                        "http://") or not plugin_url.startswith("https://"):
                    plugin_url = f"https://{plugin_url}"

                plugin_name = plugin_url.split("/")[-1].rstrip(".git")
                plugin_short = plugin_name.replace("-buildkite-plugin", "")
                plugin_dir = f"~/{plugin_name}"
                plugin_env = self.get_plugin_env(plugin_short, options)

                self.plugins[plugin_short] = {
                    "name": plugin_name,
                    "options": options,
                    "short": plugin_short,
                    "url": plugin_url,
                    "version": plugin_version,
                    "dir": plugin_dir,
                    "env": plugin_env,
                    "details": {}
                }

    def get_plugin_env(self, plugin_short: str, options: Dict[str, Any]):
        plugin_env = {}
        for option, value in options.items():
            option_name = option.replace("-", "_").upper()
            env_name = f"BUILDKITE_PLUGIN_{plugin_short.upper()}_{option_name}"
            plugin_env[env_name] = value

        plugin_env.update(self.plugin_default_env.get(plugin_short, {}))
        return plugin_env

    def install_buildkite_plugin(self, plugin: str):
        assert plugin in self.plugins

        self.logger.info(f"Installing Buildkite plugin: {plugin}")

        plugin_dir = self.plugins[plugin]["dir"]
        plugin_url = self.plugins[plugin]["url"]
        plugin_version = self.plugins[plugin]["version"]

        self.execute_ssh_command(
            f"[ ! -e {plugin_dir} ] && git clone --depth 1 "
            f"--branch {plugin_version} {plugin_url} {plugin_dir}",
            quiet=True)

    def load_plugin_details(self, plugin: str):
        assert plugin in self.plugins

        plugin_dir = self.plugins[plugin]["dir"]

        yaml_str = self.execute_ssh_command(
            f"cat {plugin_dir}/plugin.yml", quiet=True)

        details = yaml.safe_load(yaml_str)
        self.plugins[plugin]["details"] = details
        return details

    def execute_plugin_hook(self,
                            plugin: str,
                            hook: str,
                            env: Optional[Dict[str, Any]] = None,
                            script_command: Optional[str] = None):
        assert plugin in self.plugins

        self.logger.info(
            f"Executing Buildkite hook for plugin {plugin}: {hook}. "
            f"This pulls a Docker image and could take a while.")

        plugin_dir = self.plugins[plugin]["dir"]
        plugin_env = self.plugins[plugin]["env"].copy()

        if env:
            plugin_env.update(env)

        script_command = script_command or "bash -l"

        hook_script = f"{plugin_dir}/hooks/{hook}"
        self.execute_ssh_command(
            f"[ -f {hook_script} ] && cat {hook_script} | {script_command} ",
            env=plugin_env,
            as_script=False,
            quiet=True,
        )

    def print_buildkite_command(self, skipped: bool = False):
        print("-" * 80)
        print("These are the commands you need to execute to fully reproduce "
              "the run")
        print("-" * 80)
        print(self.env["BUILDKITE_COMMAND"])
        print("-" * 80)

        if skipped and self.skipped_commands:
            print("Some of the commands above have already been run. "
                  "Remaining commands:")
            print("-" * 80)
            print("\n".join(self.skipped_commands))
            print("-" * 80)

    def run_buildkite_command(self,
                              command_filter: Optional[List[str]] = None):
        commands = self.env["BUILDKITE_COMMAND"].split("\n")
        regexes = [re.compile(cf) for cf in command_filter or []]

        skipped_commands = []
        for command in commands:
            if any(rx.search(command) for rx in regexes):
                self.logger.info(f"Filtered build command: {command}")
                skipped_commands.append(command)
                continue

            self.logger.info(f"Executing build command: {command}")
            self.execute_docker_command(command)

        self.skipped_commands = skipped_commands

    def transfer_env_to_container(self):
        escaped = env_str(self.env).replace("'", "'\"'\"'")

        self.execute_docker_command(
            f"grep -q 'source ~/.env' $HOME/.bashrc "
            f"|| echo 'source ~/.env' >> $HOME/.bashrc; "
            f"echo 'export {escaped}' > $HOME/.env",
            quiet=True)

    def attach_to_container(self):
        self.logger.info("Attaching to AWS instance...")
        ssh_command = (f"ssh -ti {self.ssh_key_file} "
                       f"-o StrictHostKeyChecking=no "
                       f"-o ServerAliveInterval=30 "
                       f"{self.ssh_user}@{self.aws_instance_ip} "
                       f"'docker exec -it ray_container bash -l'")

        subprocess.run(ssh_command, shell=True)


@click.command()
@click.argument("session_url", required=False)
@click.option("-n", "--instance-name", default=None)
@click.option("-c", "--commands", is_flag=True, default=False)
@click.option("-f", "--filters", multiple=True, default=[])
def main(session_url: Optional[str],
         instance_name: Optional[str] = None,
         commands: bool = False,
         filters: Optional[List[str]] = None):
    random.seed(1235)

    logger = logging.getLogger("main")
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler()
    handler.setFormatter(
        logging.Formatter("[%(levelname)s %(asctime)s] "
                          "%(filename)s: %(lineno)d  "
                          "%(message)s"))
    logger.addHandler(handler)

    maybe_fetch_buildkite_token()
    repro = ReproSession(
        os.environ["BUILDKITE_TOKEN"],
        instance_name=instance_name,
        logger=logger)

    session_url = session_url or click.prompt(
        "Please copy and paste the Buildkite job build URI here")

    repro.set_session(session_url)

    repro.fetch_env_variables()
    repro.aws_start_instance()
    repro.aws_wait_for_instance()

    print("-" * 80)
    click.secho("Instance ID: ", nl=False)
    click.secho(repro.aws_instance_id, bold=True)
    click.secho("Instance IP: ", nl=False)
    click.secho(repro.aws_instance_ip, bold=True)
    print("-" * 80)

    logger.info(f"Instance IP: {repro.aws_instance_ip}")

    repro.prepare_instance()
    repro.docker_login()

    repro.fetch_buildkite_plugins()
    for plugin in repro.plugins:
        repro.install_buildkite_plugin(plugin)

    repro.execute_plugin_hook("dind", "pre-command")
    repro.execute_plugin_hook(
        "docker",
        "command",
        env={
            "BUILDKITE_COMMAND": "sleep infinity",
            "BUILDKITE_PLUGIN_DOCKER_TTY": "0",
            "BUILDKITE_PLUGIN_DOCKER_MOUNT_CHECKOUT": "0",
        },
        script_command=("sed -E 's/"
                        "docker run/"
                        "docker run "
                        "--cap-add=SYS_PTRACE "
                        "--name ray_container "
                        "-d/g' | "
                        "bash -l"))

    repro.create_new_ssh_client()

    repro.print_buildkite_command()

    if commands:
        filters = filters or []
        repro.run_buildkite_command(command_filter=filters)
        repro.print_buildkite_command(skipped=True)

    repro.transfer_env_to_container()

    # Print once more before attaching
    click.secho("Instance ID: ", nl=False)
    click.secho(repro.aws_instance_id, bold=True)
    click.secho("Instance IP: ", nl=False)
    click.secho(repro.aws_instance_ip, bold=True)
    print("-" * 80)

    repro.attach_to_container()

    logger.info("You are now detached from the AWS instance.")

    if click.confirm("Stop AWS instance?", default=False):
        repro.aws_stop_instance()
    else:
        repro.print_stop_command()


if __name__ == "__main__":
    main()
