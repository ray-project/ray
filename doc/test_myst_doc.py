"""Convert a jupytext-compliant format in to a python script
and execute it with parsed arguments."""

import subprocess
import argparse
import tempfile
import sys
from pathlib import Path

import jupytext

parser = argparse.ArgumentParser(description="Run a jupytext parsable file.")
parser.add_argument(
    "--path",
    help="path to the jupytext-compatible file",
)
parser.add_argument(
    "--find-recursively",
    action="store_true",
    help="if true, will attempt to find path recursively in cwd",
)

if __name__ == "__main__":

    args, remainder = parser.parse_known_args()

    path = Path(args.path)
    cwd = Path.cwd()
    if args.find_recursively and not path.exists():
        path = next((p for p in cwd.rglob("*") if str(p).endswith(args.path)), None)
    assert path and path.exists()

    with open(path, "r") as f:
        notebook = jupytext.read(f)

    name = ""
    with tempfile.NamedTemporaryFile("w", delete=False) as f:
        jupytext.write(notebook, f, fmt="py:percent")
        name = f.name

    remainder.insert(0, name)
    remainder.insert(0, sys.executable)

    # Run the notebook
    subprocess.run(remainder)
