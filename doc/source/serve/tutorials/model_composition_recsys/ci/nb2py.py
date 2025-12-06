#!/usr/bin/env python3
import argparse
import nbformat


def convert_notebook(input_path: str, output_path: str) -> None:
    """
    Read a Jupyter notebook and write a Python script, converting !serve run
    and !serve shutdown commands appropriately.
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

            # Check for serve run command
            if lines[0].lstrip().startswith("!serve run"):
                out.write("import time\n")
                out.write("from ray import serve\n")
                parts = lines[0].lstrip().split()
                # Extract app path (e.g., "serve_recommendation_pipeline:app")
                module_name, app_name = parts[2].split(":")
                out.write(f"from {module_name} import {app_name}\n")
                out.write(f"serve.run({app_name}, blocking=False)\n")
                out.write("time.sleep(300)  # Wait 5 minutes for service to be ready\n")
                out.write("\n")
            # Check for serve shutdown command
            elif lines[0].lstrip().startswith("!serve shutdown"):
                out.write("from ray import serve\n")
                out.write("serve.shutdown()\n")
                out.write("\n")
            # Check for the client_anyscale_service script
            elif lines[0].lstrip().startswith("# client_anyscale_service.py"):
                continue
            else:
                # Regular Python cell: dump as-is
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

