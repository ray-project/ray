import argparse
import click
import copy
import jsonschema
import logging
import os
from shutil import copyfile
import subprocess
import sys
import time

import ray
from ray.autoscaler.commands import (
    attach_cluster,
    exec_cluster,
    create_or_update_cluster,
    rsync,
    teardown_cluster,
)

logging.basicConfig(format=ray.ray_constants.LOGGER_FORMAT, level=logging.INFO)
logger = logging.getLogger(__file__)

# File layout for generated project files
# user-dir/
#   ray-project/
#     project.yaml
#     cluster.yaml
#     requirements.txt
PROJECT_DIR = "ray-project"
PROJECT_YAML = os.path.join(PROJECT_DIR, "project.yaml")
CLUSTER_YAML = os.path.join(PROJECT_DIR, "cluster.yaml")
REQUIREMENTS_TXT = os.path.join(PROJECT_DIR, "requirements.txt")

# File layout for templates file
# RAY/.../projects/
#   templates/
#     cluster_template.yaml
#     project_template.yaml
#     requirements.txt
_THIS_FILE_DIR = os.path.split(os.path.abspath(__file__))[0]
_TEMPLATE_DIR = os.path.join(_THIS_FILE_DIR, "templates")
PROJECT_TEMPLATE = os.path.join(_TEMPLATE_DIR, "project_template.yaml")
CLUSTER_TEMPLATE = os.path.join(_TEMPLATE_DIR, "cluster_template.yaml")
REQUIREMENTS_TXT_TEMPLATE = os.path.join(_TEMPLATE_DIR, "requirements.txt")


@click.group(
    "project", help="[Experimental] Commands working with ray project")
def project_cli():
    pass


@project_cli.command(help="Validate current project spec")
@click.option(
    "--verbose", help="If set, print the validated file", is_flag=True)
def validate(verbose):
    try:
        project = ray.projects.ProjectDefinition(os.getcwd())
        print("Project files validated!", file=sys.stderr)
        if verbose:
            print(project.config)
    except (jsonschema.exceptions.ValidationError, ValueError) as e:
        print("Validation failed for the following reason", file=sys.stderr)
        raise click.ClickException(e)


@project_cli.command(help="Create a new project within current directory")
@click.argument("project_name")
@click.option(
    "--cluster-yaml",
    help="Path to autoscaler yaml. Created by default",
    default=None)
@click.option(
    "--requirements",
    help="Path to requirements.txt. Created by default",
    default=None)
def create(project_name, cluster_yaml, requirements):
    if os.path.exists(PROJECT_DIR):
        raise click.ClickException(
            "Project directory {} already exists.".format(PROJECT_DIR))
    os.makedirs(PROJECT_DIR)

    if cluster_yaml is None:
        logger.warning("Using default autoscaler yaml")

        with open(CLUSTER_TEMPLATE) as f:
            template = f.read().replace(r"{{name}}", project_name)
        with open(CLUSTER_YAML, "w") as f:
            f.write(template)

        cluster_yaml = CLUSTER_YAML

    if requirements is None:
        logger.warning("Using default requirements.txt")
        # no templating required, just copy the file
        copyfile(REQUIREMENTS_TXT_TEMPLATE, REQUIREMENTS_TXT)

        requirements = REQUIREMENTS_TXT

    repo = None
    if os.path.exists(".git"):
        try:
            repo = subprocess.check_output(
                "git remote get-url origin".split(" ")).strip()
            logger.info("Setting repo URL to %s", repo)
        except subprocess.CalledProcessError:
            pass

    with open(PROJECT_TEMPLATE) as f:
        project_template = f.read()
        # NOTE(simon):
        # We could use jinja2, which will make the templating part easier.
        project_template = project_template.replace(r"{{name}}", project_name)
        project_template = project_template.replace(r"{{cluster}}",
                                                    cluster_yaml)
        project_template = project_template.replace(r"{{requirements}}",
                                                    requirements)
        if repo is None:
            project_template = project_template.replace(
                r"{{repo_string}}", "# repo: {}".format("..."))
        else:
            project_template = project_template.replace(
                r"{{repo_string}}", "repo: {}".format(repo))
    with open(PROJECT_YAML, "w") as f:
        f.write(project_template)


@click.group(
    "session",
    help="[Experimental] Commands working with sessions, which are "
    "running instances of a project.")
def session_cli():
    pass


def load_project_or_throw():
    # Validate the project file
    try:
        return ray.projects.ProjectDefinition(os.getcwd())
    except (jsonschema.exceptions.ValidationError, ValueError):
        raise click.ClickException(
            "Project file validation failed. Please run "
            "`ray project validate` to inspect the error.")


class SessionRunner:
    """Class for setting up a session and executing commands in it."""

    def __init__(self, session_name=None):
        """Initialize session runner and try to parse the command arguments.

        Args:
            session_name (str): Name of the session.

        Raises:
            click.ClickException: This exception is raised if any error occurs.
        """
        self.project_definition = load_project_or_throw()
        self.session_name = session_name

        # Check for features we don't support right now
        project_environment = self.project_definition.config.get(
            "environment", {})
        need_docker = ("dockerfile" in project_environment
                       or "dockerimage" in project_environment)
        if need_docker:
            raise click.ClickException(
                "Docker support in session is currently not implemented.")

    def create_cluster(self):
        """Create a cluster that will run the session."""
        create_or_update_cluster(
            config_file=self.project_definition.cluster_yaml(),
            override_min_workers=None,
            override_max_workers=None,
            no_restart=False,
            restart_only=False,
            yes=True,
            override_cluster_name=self.session_name,
        )

    def sync_files(self):
        """Synchronize files with the session."""
        rsync(
            self.project_definition.cluster_yaml(),
            source=self.project_definition.root,
            target=self.project_definition.working_directory(),
            override_cluster_name=self.session_name,
            down=False,
        )

    def setup_environment(self):
        """Set up the environment of the session."""
        project_environment = self.project_definition.config.get(
            "environment", {})

        if "requirements" in project_environment:
            requirements_txt = project_environment["requirements"]

            # Create a temporary requirements_txt in the head node.
            remote_requirements_txt = os.path.join(
                ray.utils.get_user_temp_dir(),
                "ray_project_requirements_txt_{}".format(time.time()))

            rsync(
                self.project_definition.cluster_yaml(),
                source=requirements_txt,
                target=remote_requirements_txt,
                override_cluster_name=self.session_name,
                down=False,
            )
            self.execute_command(
                "pip install -r {}".format(remote_requirements_txt))

        if "shell" in project_environment:
            for cmd in project_environment["shell"]:
                self.execute_command(cmd)

    def execute_command(self, cmd, config={}):
        """Execute a shell command in the session.

        Args:
            cmd (str): Shell command to run in the session. It will be
                run in the working directory of the project.
        """
        cwd = self.project_definition.working_directory()
        cmd = "cd {cwd}; {cmd}".format(cwd=cwd, cmd=cmd)
        exec_cluster(
            config_file=self.project_definition.cluster_yaml(),
            cmd=cmd,
            docker=False,
            screen=False,
            tmux=config.get("tmux", False),
            stop=False,
            start=False,
            override_cluster_name=self.session_name,
            port_forward=config.get("port_forward", None),
        )


def format_command(command, parsed_args):
    """Substitute arguments into command.

    Args:
        command (str): Shell comand with argument placeholders.
        parsed_args (dict): Dictionary that maps from argument names
            to their value.

    Returns:
        Shell command with parameters from parsed_args substituted.
    """
    for key, val in parsed_args.items():
        command = command.replace("{{" + key + "}}", str(val))
    return command


def get_session_runs(name, command, parsed_args):
    """Get a list of sessions to start.

    Args:
        command (str): Shell command with argument placeholders.
        parsed_args (dict): Dictionary that maps from argument names
            to their values.

    Returns:
        List of sessions to start, which are dictionaries with keys:
            "name": Name of the session to start,
            "command": Command to run after starting the session,
            "params": Parameters for this run,
            "num_steps": 4 if a command should be run, 3 if not.
    """
    if not command:
        return [{"name": name, "command": None, "params": {}, "num_steps": 3}]

    # Try to find a wildcard argument (i.e. one that has a list of values)
    # and give an error if there is more than one (currently unsupported).
    wildcard_arg = None
    for key, val in parsed_args.items():
        if isinstance(val, list):
            if not wildcard_arg:
                wildcard_arg = key
            else:
                raise click.ClickException(
                    "More than one wildcard is not supported at the moment")

    if not wildcard_arg:
        session_run = {
            "name": name,
            "command": format_command(command, parsed_args),
            "params": parsed_args,
            "num_steps": 4
        }
        return [session_run]
    else:
        session_runs = []
        for val in parsed_args[wildcard_arg]:
            parsed_args = copy.deepcopy(parsed_args)
            parsed_args[wildcard_arg] = val
            session_run = {
                "name": "{}-{}-{}".format(name, wildcard_arg, val),
                "command": format_command(command, parsed_args),
                "params": parsed_args,
                "num_steps": 4
            }
            session_runs.append(session_run)
        return session_runs


@session_cli.command(help="Attach to an existing cluster")
@click.option(
    "--screen", is_flag=True, default=False, help="Run the command in screen.")
@click.option("--tmux", help="Attach to tmux session", is_flag=True)
def attach(screen, tmux):
    project_definition = load_project_or_throw()
    attach_cluster(
        project_definition.cluster_yaml(),
        start=False,
        use_screen=screen,
        use_tmux=tmux,
        override_cluster_name=None,
        new=False,
    )


@session_cli.command(help="Stop a session based on current project config")
@click.option("--name", help="Name of the session to stop", default=None)
def stop(name):
    project_definition = load_project_or_throw()

    if not name:
        name = project_definition.config["name"]

    teardown_cluster(
        project_definition.cluster_yaml(),
        yes=True,
        workers_only=False,
        override_cluster_name=name)


@session_cli.command(
    name="start",
    context_settings=dict(ignore_unknown_options=True, ),
    help="Start a session based on current project config")
@click.argument("command", required=False)
@click.argument("args", nargs=-1, type=click.UNPROCESSED)
@click.option(
    "--shell",
    help=(
        "If set, run the command as a raw shell command instead of looking up "
        "the command in the project config"),
    is_flag=True)
@click.option("--name", help="A name to tag the session with.", default=None)
def session_start(command, args, shell, name):
    project_definition = load_project_or_throw()

    if not name:
        name = project_definition.config["name"]

    # Get the actual command to run. This also validates the command,
    # which should be done before the cluster is started.
    try:
        command, parsed_args, config = project_definition.get_command_info(
            command, args, shell, wildcards=True)
    except ValueError as e:
        raise click.ClickException(e)
    session_runs = get_session_runs(name, command, parsed_args)

    if len(session_runs) > 1 and not config.get("tmux", False):
        logging.info("Using wildcards with tmux = False would not create "
                     "sessions in parallel, so we are overriding it with "
                     "tmux = True.")
        config["tmux"] = True

    for run in session_runs:
        runner = SessionRunner(session_name=run["name"])
        logger.info("[1/{}] Creating cluster".format(run["num_steps"]))
        runner.create_cluster()
        logger.info("[2/{}] Syncing the project".format(run["num_steps"]))
        runner.sync_files()
        logger.info("[3/{}] Setting up environment".format(run["num_steps"]))
        runner.setup_environment()

        if run["command"]:
            # Run the actual command.
            logger.info("[4/4] Running command")
            runner.execute_command(run["command"], config)


@session_cli.command(
    name="commands",
    help="Print available commands for sessions of this project.")
def session_commands():
    project_definition = load_project_or_throw()
    print("Active project: " + project_definition.config["name"])
    print()

    commands = project_definition.config["commands"]

    for command in commands:
        print("Command \"{}\":".format(command["name"]))
        parser = argparse.ArgumentParser(
            command["name"], description=command.get("help"), add_help=False)
        params = command.get("params", [])
        for param in params:
            name = param.pop("name")
            if "type" in param:
                param.pop("type")
            parser.add_argument("--" + name, **param)
        help_string = parser.format_help()
        # Indent the help message by two spaces and print it.
        print("\n".join(["  " + line for line in help_string.split("\n")]))


@session_cli.command(
    name="execute",
    context_settings=dict(ignore_unknown_options=True, ),
    help="Execute a command in a session")
@click.argument("command", required=False)
@click.argument("args", nargs=-1, type=click.UNPROCESSED)
@click.option(
    "--shell",
    help=(
        "If set, run the command as a raw shell command instead of looking up "
        "the command in the project config"),
    is_flag=True)
@click.option(
    "--name", help="Name of the session to run this command on", default=None)
def session_execute(command, args, shell, name):
    project_definition = load_project_or_throw()
    try:
        command, parsed_args, config = project_definition.get_command_info(
            command, args, shell, wildcards=False)
    except ValueError as e:
        raise click.ClickException(e)

    runner = SessionRunner(session_name=name)
    command = format_command(command, parsed_args)
    runner.execute_command(command)
