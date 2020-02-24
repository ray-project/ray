"""
Test automation with [Nox | https://nox.thea.codes/]

CLI Usage:
# Run tests defined in this file
$ nox
# Reuse existing virtual environments
$ nox -r
# Run specific session
$ nox -s <session_name>
"""

import tempfile
from typing import Any

import nox
from nox.sessions import Session

python_versions = ["3.6"]


@nox.session(python=python_versions)
def tests_v8(session: Session) -> None:
    """
    Run erl tests.

    The test session only needs packages required for running the test suite. We thus
    install the erl package dependencies first and then install only the testing
    dependencies.
    """
    # Get args passed to cli invocation of nox command and pass them to pytest command
    args = session.posargs or ["-s"]
    session.run("poetry", "install", "--no-dev", external=True)
    install_ray_version(session, 8)
    session.run("pytest", *args)


@nox.session(python=python_versions)
def tests_v6(session: Session) -> None:
    """
    Run erl tests.

    The test session only needs packages required for running the test suite. We thus
    install the erl package dependencies first and then install only the testing
    dependencies.
    """
    # Get args passed to cli invocation of nox command and pass them to pytest command
    args = session.posargs or ["-s"]
    session.run("poetry", "install", "--no-dev", external=True)
    install_ray_version(session, 6)
    session.run("pytest", *args)


def install_ray_version(session: Session, version: int) -> None:
    """
    Helper function to install packages using session.install with constraints defined
    in poetry.lock file.

    :param session: nox.Session
    :param version: 6 for ray 0.6.6 or 8 for ray 0.8.1
    """
    ray_version = "0.6.6" if version == 6 else "0.8.1"
    session.install(f"ray[rllib]=={ray_version}")
