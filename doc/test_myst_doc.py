"""Convert a jupytext-compliant format in to a python script
and execute it with parsed arguments."""

import subprocess
import argparse
import tempfile
import sys

import jupytext

parser = argparse.ArgumentParser(description="Run a jupytext parsable file.")
parser.add_argument(
    "--path",
    help="path to the jupytext-compatible file",
)

if __name__ == "__main__":

    args, remainder = parser.parse_known_args()

    with open(args.path, "r") as f:
        notebook = jupytext.read(f)

    name = ""
    with tempfile.NamedTemporaryFile("w", delete=False) as f:
        jupytext.write(notebook, f, fmt="py:percent")
        name = f.name

    remainder.insert(0, name)
    remainder.insert(0, sys.executable)

    # Run the notebook
    subprocess.run(remainder)
