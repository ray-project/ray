"""Build a standalone raydepsets zipapp binary.

Run via: bazel run //ci/raydepsets:build_raydepsets_binary -- /output/path/raydepsets
"""
import importlib
import os
import shutil
import sys
import tempfile
import zipapp
from pathlib import Path

# Packages to vendor into the zipapp (these are runtime deps of raydepsets).
VENDOR_PACKAGES = [
    "click",
    "networkx",
    "pip_requirements_parser",
    "packaging",  # transitive dep of pip_requirements_parser
    "pyparsing",  # transitive dep of pip_requirements_parser
    "yaml",  # pyyaml installs as 'yaml'
]

# raydepsets source files (relative to ci/raydepsets/).
RAYDEPSETS_SOURCES = [
    "__init__.py",
    "__main__.py",
    "cli.py",
    "workspace.py",
]


def _find_package_path(package_name: str) -> Path:
    """Find the installed location of a Python package."""
    mod = importlib.import_module(package_name)
    mod_file = getattr(mod, "__file__", None)
    if mod_file is not None:
        mod_path = Path(mod_file)
        if mod_path.name.startswith("__init__"):
            return mod_path.parent
        return mod_path
    # Namespace package or C extension with __path__
    mod_path = getattr(mod, "__path__", None)
    if mod_path:
        return Path(mod_path[0])
    raise RuntimeError(f"Cannot locate package: {package_name}")


def _copy_package(package_name: str, dest: Path):
    """Copy a package into the staging directory."""
    src = _find_package_path(package_name)
    if src.is_dir():
        dest_dir = dest / src.name
        shutil.copytree(src, dest_dir, dirs_exist_ok=True)
    else:
        # Single-file module (e.g. yaml might be a single .py in some installs)
        shutil.copy2(src, dest / src.name)


def _find_raydepsets_source_dir() -> Path:
    """Find the raydepsets source directory (works under Bazel runfiles)."""
    # When run under Bazel, our sources are in the runfiles tree.
    # Try to find cli.py relative to this script.
    this_dir = Path(__file__).resolve().parent
    if (this_dir / "cli.py").exists():
        return this_dir
    raise RuntimeError(f"Cannot find raydepsets sources. Expected cli.py in {this_dir}")


def main():
    if len(sys.argv) < 2:
        print(
            "Usage: bazel run //ci/raydepsets:build_raydepsets_binary -- /output/path/raydepsets",
            file=sys.stderr,
        )
        sys.exit(1)

    output_path = Path(sys.argv[1]).resolve()
    source_dir = _find_raydepsets_source_dir()
    staging = Path(tempfile.mkdtemp())

    try:
        # 1. Vendor dependencies
        for pkg in VENDOR_PACKAGES:
            try:
                _copy_package(pkg, staging)
            except (ImportError, RuntimeError) as e:
                print(f"Error: could not vendor {pkg}: {e}", file=sys.stderr)
                sys.exit(1)

        # 2. Copy raydepsets sources
        pkg_dir = staging / "raydepsets"
        pkg_dir.mkdir(exist_ok=True)
        for src_file in RAYDEPSETS_SOURCES:
            src_path = source_dir / src_file
            if not src_path.exists():
                print(f"Error: source file not found: {src_path}", file=sys.stderr)
                sys.exit(1)
            shutil.copy2(src_path, pkg_dir / src_file)

        # 3. Create top-level __main__.py entry point
        main_py = staging / "__main__.py"
        main_py.write_text(
            "import sys, os\n"
            "sys.path.insert(0, os.path.dirname(__file__))\n"
            "from raydepsets.cli import cli\n"
            "cli()\n"
        )

        # 4. Build zipapp
        output_path.parent.mkdir(parents=True, exist_ok=True)
        zipapp.create_archive(
            staging,
            target=output_path,
            interpreter="/usr/bin/env python3",
        )
        os.chmod(output_path, 0o755)
        print(f"Built standalone binary: {output_path}")
    finally:
        shutil.rmtree(staging)


if __name__ == "__main__":
    main()
