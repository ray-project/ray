"""
This script ensures python files conform to ray's import ordering rules.
In particular, we make sure psutil and setproctitle is imported _after_
importing ray due to our bundling of the two libraries.

Usage:
$ python check_import_order.py SOURCE_DIR -s SKIP_DIR
some/file/path.py:23 import psutil without explicitly import ray before it.
"""

import argparse
import glob
import io
import re
import sys
from pathlib import Path

exit_with_error = False


def check_import(file):
    check_to_lines = {"import ray": -1, "import psutil": -1, "import setproctitle": -1}

    with io.open(file, "r", encoding="utf-8") as f:
        for i, line in enumerate(f):
            for check in check_to_lines.keys():
                # This regex will match the following case
                # - the string itself: `import psutil`
                # - white space/indentation + the string:`    import psutil`
                # - the string and arbitrary whitespace: `import psutil    `
                # - the string and the noqa flag to silent pylint
                #   `import psutil # noqa F401 import-ordering`
                # It will not match the following
                # - submodule import: `import ray.constants as ray_constants`
                # - submodule import: `from ray import xyz`
                if (
                    re.search(r"^\s*" + check + r"(\s*|\s+# noqa F401.*)$", line)
                    and check_to_lines[check] == -1
                ):
                    check_to_lines[check] = i

    for import_lib in ["import psutil", "import setproctitle"]:
        if check_to_lines[import_lib] != -1:
            import_psutil_line = check_to_lines[import_lib]
            import_ray_line = check_to_lines["import ray"]
            if import_ray_line == -1 or import_ray_line > import_psutil_line:
                print(
                    "{}:{}".format(str(file), import_psutil_line + 1),
                    "{} without explicitly import ray before it.".format(import_lib),
                )
                global exit_with_error
                exit_with_error = True


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("path", help="File path to check. e.g. '.' or './src'")
    # TODO(simon): For the future, consider adding a feature to explicitly
    # white-list the path instead of skipping them.
    parser.add_argument("-s", "--skip", action="append", help="Skip certian directory")
    args = parser.parse_args()

    file_path = Path(args.path)
    if file_path.is_dir():
        all_py_files = glob.glob("*.py", recursive=True)
    else:
        all_py_files = [file_path]

    if args.skip is not None:
        filtered_py_files = []
        for py_file in all_py_files:
            should_skip = False
            for skip_dir in args.skip:
                if str(py_file).startswith(skip_dir):
                    should_skip = True
            if not should_skip:
                filtered_py_files.append(py_file)
        all_py_files = filtered_py_files

    for py_file in all_py_files:
        check_import(py_file)

    if exit_with_error:
        print("check import ordering failed")
        sys.exit(1)
