#!/usr/bin/env python3
import argparse

import nbformat


def convert_notebook(input_path: str, output_path: str) -> None:
    """
    Read a Jupyter notebook and write a Python script, converting all %%bash
    cells and IPython "!" commands into subprocess.run calls that raise on error.
    Cells that load or autoreload extensions are ignored.
    """
    nb = nbformat.read(input_path, as_version=4)
    with open(output_path, "w") as out:
        for cell in nb.cells:
            # Only process code cells
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

            # Detect a %%bash cell
            if lines and lines[0].strip().startswith("%%bash"):
                bash_script = "\n".join(lines[1:]).rstrip()
                out.write("import subprocess\n")
                out.write(
                    f"subprocess.run(r'''{bash_script}''',\n"
                    "               shell=True,\n"
                    "               check=True,\n"
                    "               executable='/bin/bash')\n\n"
                )
            else:
                # Detect any IPython '!' shell commands in code lines
                has_bang = any(line.lstrip().startswith("!") for line in lines)
                if has_bang:
                    out.write("import subprocess\n")
                    for line in lines:
                        stripped = line.lstrip()
                        if stripped.startswith("!"):
                            cmd = stripped[1:].lstrip()
                            out.write(
                                f"subprocess.run(r'''{cmd}''',\n"
                                "               shell=True,\n"
                                "               check=True,\n"
                                "               executable='/bin/bash')\n"
                            )
                        else:
                            out.write(line.rstrip() + "\n")
                    out.write("\n")
                else:
                    # Regular Python cell: dump as-is
                    out.write(cell.source.rstrip() + "\n\n")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Convert a Jupyter notebook to a Python script, preserving bash cells and '!' commands as subprocess calls."
    )
    parser.add_argument("input_nb", help="Path to the input .ipynb file")
    parser.add_argument("output_py", help="Path for the output .py script")
    args = parser.parse_args()
    convert_notebook(args.input_nb, args.output_py)


if __name__ == "__main__":
    main()
