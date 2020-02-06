import argparse
import re
import sys
import tempfile
from pathlib import Path

exit_with_error = False


def check_import(file):
    check_to_lines = {
        "import ray": -1,
        "import psutil": -1,
        "import setproctitle": -1
    }

    with open(file) as f:
        for i, line in enumerate(f):
            for check in check_to_lines.keys():
                if re.match(r"\s+" + check + r"(\s|$)", line):
                    check_to_lines[check] = i

    for import_lib in ["import psutil", "import setproctitle"]:
        if check_to_lines[import_lib] != -1:
            import_psutil_line = check_to_lines[import_lib]
            import_ray_line = check_to_lines["import ray"]
            if import_ray_line == -1 or import_ray_line > import_psutil_line:
                print(
                    "{}:{}".format(str(file), import_psutil_line + 1),
                    "{} without explicit import ray before it.".format(
                        import_lib))
                global exit_with_error
                exit_with_error = True


# Run the test with pytest file_name
def test_check_import():
    global exit_with_error
    _, path = tempfile.mkstemp()

    with open(path, "w") as f:
        f.write("""
        import psutil
        import ray
        """)
    check_import(path)
    assert exit_with_error
    exit_with_error = False

    with open(path, "w") as f:
        f.write("""
        import psutil
        """)
    check_import(path)
    assert exit_with_error
    exit_with_error = False

    with open(path, "w") as f:
        f.write("""
        import random_lib
        """)
    check_import(path)
    assert not exit_with_error
    exit_with_error = False

    with open(path, "w") as f:
        f.write("""
        import setproctitle
        import ray
        """)
    check_import(path)
    assert exit_with_error
    exit_with_error = False

    with open(path, "w") as f:
        f.write("""
        import ray
        import psutil
        """)
    check_import(path)
    assert not exit_with_error
    exit_with_error = False


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("path", help="File path to check. e.g. '.' or './src'")
    parser.add_argument(
        "-s", "--skip", action="append", help="Skip certian directory")
    args = parser.parse_args()

    file_path = Path(args.path)
    if file_path.is_dir():
        all_py_files = file_path.rglob("*.py")
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
