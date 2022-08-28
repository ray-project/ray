"""Convert a jupytext-compliant format in to a python script
and execute it with parsed arguments.

Any cell with 'remove-cell-ci' tag in metadata will not be included
in the converted python script.
"""

import argparse
import subprocess
import sys
import tempfile
from pathlib import Path

import jupytext

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument(
    "--path",
    help="path to the jupytext-compatible file",
)
parser.add_argument(
    "--find-recursively",
    action="store_true",
    help="if true, will attempt to find path recursively in cwd",
)
parser.add_argument(
    "--no-postprocess",
    action="store_true",
    help="if true, will not postprocess the notebook",
)


def filter_out_cells_with_remove_cell_ci_tag(cells: list):
    """Filters out cells which contain the 'remove-cell-ci' tag in metadata"""

    def should_keep_cell(cell):
        tags = cell.metadata.get("tags")
        if tags:
            # Both - and _ for consistent behavior with built-in tags
            return "remove_cell_ci" not in tags and "remove-cell-ci" not in tags
        return True

    return [cell for cell in cells if should_keep_cell(cell)]


def postprocess_notebook(notebook):
    notebook.cells = filter_out_cells_with_remove_cell_ci_tag(notebook.cells)
    return notebook


if __name__ == "__main__":

    args, remainder = parser.parse_known_args()

    path = Path(args.path)
    cwd = Path.cwd()
    if args.find_recursively and not path.exists():
        path = next((p for p in cwd.rglob("*") if str(p).endswith(args.path)), None)
    assert path and path.exists()

    with open(path, "r") as f:
        notebook = jupytext.read(f)

    if not args.no_postprocess:
        notebook = postprocess_notebook(notebook)

    name = ""
    with tempfile.NamedTemporaryFile("w", delete=False) as f:
        jupytext.write(notebook, f, fmt="py:percent")
        name = f.name

    remainder.insert(0, name)
    remainder.insert(0, sys.executable)

    # Run the notebook
    subprocess.run(remainder, check=True)
