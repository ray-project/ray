from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import logging
import os
import sys
from shutil import copyfile
import time
import click
import jsonschema

import ray
from ray.autoscaler.commands import (
    attach_cluster,
    exec_cluster,
    create_or_update_cluster,
    rsync,
    teardown_cluster,
)

logging.basicConfig(format=ray.ray_constants.LOGGER_FORMAT)
logger = logging.getLogger(__file__)

# File layout for generated project files
# user-dir/
#   .rayproject/
#     project.yaml
#     cluster.yaml
#     requirements.txt
PROJECT_DIR = ".rayproject"
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
        project = ray.projects.load_project(os.getcwd())
        print("Project files validated!", file=sys.stderr)
        if verbose:
            print(project)
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
        logger.warn("Using default autoscaler yaml")

        with open(CLUSTER_TEMPLATE) as f:
            template = f.read().replace(r"{{name}}", project_name)
        with open(CLUSTER_YAML, "w") as f:
            f.write(template)

        cluster_yaml = CLUSTER_YAML

    if requirements is None:
        logger.warn("Using default requirements.txt")
        # no templating required, just copy the file
        copyfile(REQUIREMENTS_TXT_TEMPLATE, REQUIREMENTS_TXT)

        requirements = REQUIREMENTS_TXT

    with open(PROJECT_TEMPLATE) as f:
        project_template = f.read()
        # NOTE(simon):
        # We could use jinja2, which will make the templating part easier.
        project_template = project_template.replace(r"{{name}}", project_name)
        project_template = project_template.replace(r"{{cluster}}",
                                                    cluster_yaml)
        project_template = project_template.replace(r"{{requirements}}",
                                                    requirements)

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
        return ray.projects.load_project(os.getcwd())
    except (jsonschema.exceptions.ValidationError, ValueError):
        raise click.ClickException(
            "Project file validation failed. Please run "
            "`ray project validate` to inspect the error.")


@session_cli.command(help="Attach to an existing cluster")
def attach():
    project_definition = load_project_or_throw()
    attach_cluster(
        project_definition["cluster"],
        start=False,
        use_tmux=False,
        override_cluster_name=None,
        new=False,
    )


@session_cli.command(help="Stop a session based on current project config")
def stop():
    project_definition = load_project_or_throw()
    teardown_cluster(
        project_definition["cluster"],
        yes=True,
        workers_only=False,
        override_cluster_name=None)


@session_cli.command(help="Start a session based on current project config")
def start():
    project_definition = load_project_or_throw()

    # Check for features we don't support right now
    project_environment = project_definition["environment"]
    need_docker = ("dockerfile" in project_environment
                   or "dockerimage" in project_environment)
    if need_docker:
        raise click.ClickException(
            "Docker support in session is currently not implemented. "
            "Please file an feature request at"
            "https://github.com/ray-project/ray/issues")

    cluster_yaml = project_definition["cluster"]
    working_directory = project_definition["name"]

    logger.info("[1/4] Creating cluster")
    create_or_update_cluster(
        config_file=cluster_yaml,
        override_min_workers=None,
        override_max_workers=None,
        no_restart=False,
        restart_only=False,
        yes=True,
        override_cluster_name=None,
    )

    logger.info("[2/4] Syncing the repo")
    if "repo" in project_definition:
        # HACK: Skip git clone if exists so the this command can be idempotent
        # More advanced repo update behavior can be found at
        # https://github.com/jupyterhub/nbgitpuller/blob/master/nbgitpuller/pull.py
        session_exec_cluster(
            cluster_yaml,
            "git clone {repo} {directory} || true".format(
                repo=project_definition["repo"],
                directory=project_definition["name"]),
        )
    else:
        session_exec_cluster(
            cluster_yaml,
            "mkdir {directory} || true".format(
                directory=project_definition["name"]))

    logger.info("[3/4] Setting up environment")
    _setup_environment(
        cluster_yaml, project_definition["environment"], cwd=working_directory)

    logger.info("[4/4] Running commands")
    _run_commands(
        cluster_yaml, project_definition["commands"], cwd=working_directory)


def session_exec_cluster(cluster_yaml, cmd, cwd=None):
    if cwd is not None:
        cmd = "cd {cwd}; {cmd}".format(cwd=cwd, cmd=cmd)
    exec_cluster(
        config_file=cluster_yaml,
        cmd=cmd,
        docker=False,
        screen=False,
        tmux=False,
        stop=False,
        start=False,
        override_cluster_name=None,
        port_forward=None,
    )


def _setup_environment(cluster_yaml, project_environment, cwd):

    if "requirements" in project_environment:
        requirements_txt = project_environment["requirements"]

        # Create a temporary requirements_txt in the head node.
        remote_requirements_txt = (
            "/tmp/" + "ray_project_requirements_txt_{}".format(time.time()))

        rsync(
            cluster_yaml,
            source=requirements_txt,
            target=remote_requirements_txt,
            override_cluster_name=None,
            down=False,
        )
        session_exec_cluster(
            cluster_yaml,
            "pip install -r {}".format(remote_requirements_txt),
            cwd=cwd)

    if "shell" in project_environment:
        for cmd in project_environment["shell"]:
            session_exec_cluster(cluster_yaml, cmd, cwd=cwd)


def _run_commands(cluster_yaml, commands, cwd):
    for cmd in commands:
        logger.debug("Running {}".format(cmd["name"]))
        session_exec_cluster(cluster_yaml, cmd["command"], cwd=cwd)
