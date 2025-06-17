#!/usr/bin/env python3
import argparse

import nbformat


def convert_notebook(input_path: str, output_path: str) -> None:
    """
    Reads a Jupyter notebook and converts it into a Python script.

    This function processes each code cell from the input notebook:
    - Converts cells starting with `%%bash` (cell magic) into a
      single `subprocess.run` call that executes the cell's content as a bash script.
    - For other code cells, converts lines starting with `!` (IPython shell escape)
      into individual `subprocess.run` calls.
    - Writes regular Python code lines to the script as is.

    All `subprocess.run` calls use `shell=True`, `check=True`
    (raising an exception on non-zero exit codes), and `executable='/bin/bash'`.
    The function adds the `import subprocess` statement once at the beginning of the
    script if it finds any bash commands or shell escapes.
    """
    nb = nbformat.read(input_path, as_version=4)
    # Flag to ensure 'import subprocess' is written only once in the output script.
    subprocess_imported = False

    with open(output_path, "w") as out:
        for cell in nb.cells:
            if cell.cell_type != "code":
                continue

            source_lines = cell.source.splitlines()

            # Handle %%bash magic: convert the entire cell to a subprocess call.
            if source_lines and source_lines[0].strip().startswith("%%bash"):
                if not subprocess_imported:
                    out.write("import subprocess\n")
                    subprocess_imported = True

                # Extract the script, removing the '%%bash' line and any trailing whitespace.
                bash_script = "\n".join(source_lines[1:]).rstrip()
                out.write(
                    f"subprocess.run(r'''{bash_script}''',\n"
                    "               shell=True,\n"
                    "               check=True,\n"
                    "               executable='/bin/bash')\n\n"
                )
            else:
                # For non-%%bash cells, check for IPython '!' shell escapes within the cell.
                # Convert these lines individually.
                is_shell_escape_cell = any(
                    line.lstrip().startswith("!") for line in source_lines
                )

                if is_shell_escape_cell:
                    if not subprocess_imported:
                        out.write("import subprocess\n")
                        subprocess_imported = True

                    # Process line by line, converting '!' lines to subprocess calls
                    # and writing other Python lines as is.
                    for line in source_lines:
                        stripped_line = line.lstrip()
                        if stripped_line.startswith("!"):
                            # Extract command after '!' and any leading whitespace on that part.
                            command = stripped_line[1:].lstrip()
                            out.write(
                                f"subprocess.run(r'''{command}''',\n"
                                "               shell=True,\n"
                                "               check=True,\n"
                                "               executable='/bin/bash')\n"
                            )
                        else:
                            # Write regular Python lines, preserving original trailing whitespace
                            # and adding a newline.
                            out.write(line.rstrip() + "\n")
                    out.write(
                        "\n"
                    )  # Add a blank line after processing this mixed-content cell.
                else:
                    # Regular Python code cell: write its content directly.
                    # rstrip() removes trailing newlines from the cell source,
                    # then add two newlines for proper separation between cell blocks.
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
