"""Execute a jupytext markdown notebook."""

import subprocess
import argparse
import tempfile
import sys

import jupytext

parser = argparse.ArgumentParser(description="Run a jupytext parsable markdown file.")
parser.add_argument(
    "--path",
    help="path to the markdown file",
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
