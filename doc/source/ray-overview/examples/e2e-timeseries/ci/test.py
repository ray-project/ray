#!/usr/bin/env python3
"""
Script to convert Jupyter notebooks to Python scripts and execute them in order.

This script:
1. Finds all .ipynb files in the specified target directory.
2. Converts them to .py files using the nb2py.py converter.
3. Executes the generated Python scripts in numerical order (01*.py, 02*.py, etc.).
4. Fails if any script execution fails.
"""

import argparse
import glob
import os
import re
import subprocess
import sys
from pathlib import Path
from typing import List


def find_notebooks(target_dir: str) -> List[Path]:
    """Find all .ipynb files in the target directory and sort them numerically."""
    pattern = os.path.join(target_dir, "*.ipynb")
    notebook_paths = glob.glob(pattern)

    def numerical_sort_key(path_str: str) -> tuple:
        """Extract numerical prefix for proper sorting (e.g., '01-' -> 1)."""
        filename = os.path.basename(path_str)
        # Look for numerical prefix at the start of filename.
        match = re.match(r"^(\d+)", filename)
        if match:
            return (int(match.group(1)), filename)
        else:
            # Files without numerical prefix go last, sorted alphabetically.
            return (float("inf"), filename)

    # Sort by numerical prefix first, then by filename.
    sorted_paths = sorted(notebook_paths, key=numerical_sort_key)
    return [Path(p) for p in sorted_paths]


def convert_notebook_to_script(notebook_path: Path, nb2py_script: Path) -> Path:
    """Convert a single notebook to a Python script using nb2py.py."""
    output_path = notebook_path.with_suffix(".py")

    print(f"Converting {notebook_path} to {output_path}")

    try:
        subprocess.run(
            [sys.executable, str(nb2py_script), str(notebook_path), str(output_path)],
            check=True,
            capture_output=True,
            text=True,
        )
    except subprocess.CalledProcessError as e:
        print(f"Error converting {notebook_path}: {e}")
        print(f"stdout: {e.stdout}")
        print(f"stderr: {e.stderr}")
        raise

    return output_path


def execute_script(script_path: Path, working_dir: Path) -> None:
    """Execute a Python script and raise an exception if it fails."""
    print(f"Executing {script_path}")

    try:
        result = subprocess.run(
            [sys.executable, str(script_path.resolve())],
            check=True,
            capture_output=True,
            text=True,
            cwd=str(working_dir),
        )

        # Print output for visibility.
        if result.stdout:
            print(f"stdout from {script_path.name}:")
            print(result.stdout)
        if result.stderr:
            print(f"stderr from {script_path.name}:")
            print(result.stderr)

    except subprocess.CalledProcessError as e:
        print(f"Error executing {script_path}: {e}")
        print(f"stdout: {e.stdout}")
        print(f"stderr: {e.stderr}")
        raise


def cleanup_generated_scripts(script_paths: List[Path]) -> None:
    """Remove generated Python scripts."""
    for script_path in script_paths:
        if script_path.exists():
            print(f"Cleaning up {script_path}")
            script_path.unlink()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Convert Jupyter notebooks to Python scripts and execute them in order."
    )
    parser.add_argument(
        "target_dir",
        help="Directory containing the .ipynb files to convert and execute",
    )
    parser.add_argument(
        "--nb2py-script",
        default="ci/nb2py.py",
        help="Path to the nb2py.py conversion script (default: ci/nb2py.py)",
    )

    args = parser.parse_args()

    # Get absolute paths and validate inputs.
    target_dir = Path(args.target_dir).resolve()
    if not target_dir.exists() or not target_dir.is_dir():
        print(
            f"Error: Target directory {target_dir} does not exist or is not a directory"
        )
        sys.exit(1)

    nb2py_script = Path(args.nb2py_script).resolve()
    if not nb2py_script.exists():
        print(f"Error: nb2py script {nb2py_script} does not exist")
        sys.exit(1)

    notebooks = find_notebooks(str(target_dir))
    if not notebooks:
        print(f"No .ipynb files found in {target_dir}")
        return

    print(f"Found {len(notebooks)} notebooks: {[nb.name for nb in notebooks]}")

    generated_scripts = []

    try:
        # Convert all notebooks to Python scripts.
        for notebook in notebooks:
            script_path = convert_notebook_to_script(notebook, nb2py_script)
            generated_scripts.append(script_path)

        print(
            f"\nSuccessfully converted {len(generated_scripts)} notebooks to Python scripts"
        )

        # Execute scripts in order from the target directory.
        print("\nExecuting scripts in order...")
        for script_path in generated_scripts:
            execute_script(script_path, target_dir)

        print(f"\nSuccessfully executed all {len(generated_scripts)} scripts!")

    except Exception as e:
        print(f"\nExecution failed: {e}")
        sys.exit(1)

    finally:
        cleanup_generated_scripts(generated_scripts)


if __name__ == "__main__":
    main()
