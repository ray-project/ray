#!/usr/bin/env python3
import argparse
import nbformat


def convert_notebook(
    input_path: str, output_path: str, ignore_cmds: bool = False
) -> None:
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

            if lines:
                # Detect any IPython '!' shell commands in code lines
                has_bang = any(line.lstrip().startswith("!") for line in lines)
                # Detect %pip magic commands
                has_pip_magic = any(line.lstrip().startswith("%pip") for line in lines)
                # Start with "serve run" "serve shutdown" "curl" or "anyscale service" commands
                to_ignore_cmd = (
                    "serve run",
                    "serve shutdown",
                    "curl",
                    "anyscale service",
                )
                has_ignored_start = any(
                    line.lstrip().startswith(to_ignore_cmd) for line in lines
                )
                # Skip %pip cells entirely
                if has_pip_magic:
                    continue
                if has_bang or has_ignored_start:
                    if ignore_cmds:
                        continue
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
                    # Regular Python cell:
                    code = cell.source.rstrip()
                    if "ds_large = ds.limit(1_000_000)" in code:
                        # Instead of testing a large dataset in CI, test a small dataset
                        code = code.replace("ds.limit(1_000_000)", "ds.limit(10_000)")
                    # else, dump as-is
                    out.write(code + "\n\n")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Convert a Jupyter notebook to a Python script, preserving bash cells and '!' commands as subprocess calls unless ignored with --ignore-cmds."
    )
    parser.add_argument("input_nb", help="Path to the input .ipynb file")
    parser.add_argument("output_py", help="Path for the output .py script")
    parser.add_argument(
        "--ignore-cmds", action="store_true", help="Ignore bash cells and '!' commands"
    )
    args = parser.parse_args()
    convert_notebook(args.input_nb, args.output_py, ignore_cmds=args.ignore_cmds)


if __name__ == "__main__":
    main()
