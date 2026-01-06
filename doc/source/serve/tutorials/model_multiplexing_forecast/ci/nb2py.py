#!/usr/bin/env python3
import argparse
import nbformat


def convert_notebook(
    input_path: str, output_path: str, ignore_cmds: bool = False
) -> None:
    """
    Convert a Jupyter notebook to a Python script for CI testing.
    
    - Converts !serve run to serve.run() with 5-minute sleep
    - Converts !serve shutdown to serve.shutdown()
    - Keeps all other code cells as-is
    """
    nb = nbformat.read(input_path, as_version=4)
    with open(output_path, "w") as out:
        for cell in nb.cells:
            # Only process code cells
            if cell.cell_type != "code":
                continue

            lines = cell.source.splitlines()
            if not lines:
                continue

            # Check for anyscale commands (expensive/redundant to test in CI)
            if lines[0].lstrip().startswith("# client_anyscale_service.py"):
                continue
            else:   
                # Dump regular Python cell as-is
                out.write(cell.source.rstrip() + "\n\n")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Convert a Jupyter notebook to a Python script, converting serve commands to Python API calls."
    )
    parser.add_argument("input_nb", help="Path to the input .ipynb file")
    parser.add_argument("output_py", help="Path for the output .py script")
    args = parser.parse_args()
    convert_notebook(args.input_nb, args.output_py)


if __name__ == "__main__":
    main()
