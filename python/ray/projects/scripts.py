from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import logging
import os
import sys
from shutil import copyfile

import click
import jsonschema

import ray

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
        print("🍰 Project files validated!", file=sys.stderr)
        if verbose:
            print(project)
    except (jsonschema.exceptions.ValidationError, ValueError) as e:
        print("💔 Validation failed for the following reason", file=sys.stderr)
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
    "session", help="[Experimental] Commands working with ray session")
def session_cli():
    pass
