#!/usr/bin/env python3
import argparse
import nbformat


def convert_notebook(input_path: str, output_path: str) -> None:
    """
    Read a Jupyter notebook and write a Python script.
    Cells that load or autoreload extensions are ignored.
    """
    nb = nbformat.read(input_path, as_version=4)
    with open(output_path, "w") as out:
        for cell in nb.cells:
            if cell.cell_type != "code":
                continue

            lines = cell.source.splitlines()
            # Skip cells that load or autoreload extensions
            if any(
                l.strip().startswith("%load_ext autoreload")
                or l.strip().startswith("%autoreload all")
                for l in lines
            ):
                continue

            out.write(cell.source.rstrip() + "\n\n")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Convert a Jupyter notebook to a Python script."
    )
    parser.add_argument("input_nb", help="Path to the input .ipynb file")
    parser.add_argument("output_py", help="Path for the output .py script")
    args = parser.parse_args()
    convert_notebook(args.input_nb, args.output_py)


if __name__ == "__main__":
    main()
