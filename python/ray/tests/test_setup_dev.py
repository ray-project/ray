"""Tests for ``python/ray/setup-dev.py``, the Python-only development setup.

The script links the packages listed in its ``PACKAGES_TO_LINK`` constant from
the source tree into the installed Ray package. These tests drive ``do_link``
with temporary source and install directories, so they never touch the
checked-out tree or a real Ray installation.

The ``serve``-specific handling of the generated directory is not covered here,
because it operates on the fixed ``/tmp/ray/_serve`` path that is shared with
running Ray instances. Whether the ``PACKAGES_TO_LINK`` entries still exist in
the source tree is checked by ``ci/lint/check_setup_dev_packages.py``, which
runs in the lint pipeline where the full checkout is available: CI test images
mask most of the Ray libraries, so a test running under Bazel cannot see them.
"""

import importlib.util
import os
import shutil
import sys
from pathlib import Path

import pytest

import ray

SETUP_DEV_PATH = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "setup-dev.py")
)

requires_symlinks = pytest.mark.skipif(
    sys.platform == "win32",
    reason="setup-dev.py creates junctions instead of symlinks on Windows.",
)


@pytest.fixture
def setup_dev():
    spec = importlib.util.spec_from_file_location(
        "setup_dev_under_test", SETUP_DEV_PATH
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def installed_ray(tmp_path, monkeypatch, setup_dev):
    installed_ray_dir = tmp_path / "installed" / "ray"
    installed_ray_dir.mkdir(parents=True)
    monkeypatch.setattr(ray, "__file__", str(installed_ray_dir / "__init__.py"))
    return installed_ray_dir


@pytest.fixture
def source_package(tmp_path):
    package_dir = tmp_path / "source" / "example_pkg"
    package_dir.mkdir(parents=True)
    (package_dir / "from_source.py").write_text("")
    return package_dir


def test_do_link_skips_packages_in_skip_list(
    setup_dev, installed_ray, source_package, capsys
):
    setup_dev.do_link(
        "example_pkg",
        force=True,
        skip_list=["example_pkg"],
        local_path=str(source_package),
    )

    assert "Skip creating symbolic link for example_pkg" in capsys.readouterr().out
    assert not (installed_ray / "example_pkg").exists()


def test_do_link_skips_packages_not_in_allow_list(
    setup_dev, installed_ray, source_package, capsys
):
    setup_dev.do_link(
        "example_pkg",
        force=True,
        allow_list=["another_pkg"],
        local_path=str(source_package),
    )

    assert (
        "Skip creating symbolic link for example_pkg (not in allow list)"
        in capsys.readouterr().out
    )
    assert not (installed_ray / "example_pkg").exists()


def test_do_link_checks_that_the_source_exists(setup_dev, installed_ray, tmp_path):
    with pytest.raises(AssertionError):
        setup_dev.do_link(
            "example_pkg",
            force=True,
            local_path=str(tmp_path / "missing_pkg"),
        )


def test_do_link_keeps_package_when_confirmation_is_declined(
    setup_dev, installed_ray, source_package, monkeypatch
):
    package_home = installed_ray / "example_pkg"
    package_home.mkdir()
    (package_home / "installed_only.py").write_text("")
    monkeypatch.setattr(setup_dev.click, "confirm", lambda *args, **kwargs: False)

    setup_dev.do_link("example_pkg", force=False, local_path=str(source_package))

    assert not package_home.is_symlink()
    assert (package_home / "installed_only.py").exists()
    assert not Path(f"{package_home}.bak").exists()


@requires_symlinks
def test_do_link_replaces_installed_package_with_symlink(
    setup_dev, installed_ray, source_package
):
    package_home = installed_ray / "example_pkg"
    package_home.mkdir()
    (package_home / "installed_only.py").write_text("")

    setup_dev.do_link("example_pkg", force=True, local_path=str(source_package))

    assert package_home.is_symlink()
    assert os.path.realpath(package_home) == os.path.realpath(source_package)
    assert (package_home / "from_source.py").exists()
    backup = Path(f"{package_home}.bak")
    assert backup.is_dir()
    assert (backup / "installed_only.py").exists()


@requires_symlinks
def test_do_link_is_idempotent_when_already_linked(
    setup_dev, installed_ray, source_package, capsys
):
    package_home = installed_ray / "example_pkg"
    package_home.mkdir()
    (package_home / "installed_only.py").write_text("")

    setup_dev.do_link("example_pkg", force=True, local_path=str(source_package))
    backup = Path(f"{package_home}.bak")
    shutil.rmtree(backup)

    # Drop the first call's output so the assertion below only covers the
    # second call.
    capsys.readouterr()

    setup_dev.do_link("example_pkg", force=True, local_path=str(source_package))

    assert "already linked to source" in capsys.readouterr().out
    assert not backup.exists()
    assert package_home.is_symlink()
    assert os.path.realpath(package_home) == os.path.realpath(source_package)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
