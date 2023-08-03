"""Execute a jupytext markdown notebook"""

import runpy
import argparse
import tempfile

import jupytext

parser = argparse.ArgumentParser(description="Run a jupytext parsable markdown file.")
parser.add_argument(
    "--path",
    help="path to the markdown file",
)

if __name__ == "__main__":
    args = parser.parse_args()

    with open(args.path, "r") as f:
        notebook = jupytext.read(f)

    name = ""
    with tempfile.NamedTemporaryFile("w", delete=False) as f:
        jupytext.write(notebook, f, fmt="py:percent")
        name = f.name

    # execute the test
    runpy.run_path(name)
