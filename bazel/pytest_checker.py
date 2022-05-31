import argparse
import re
from pathlib import Path


def check_file(file_contents: str) -> bool:
    return bool(re.search(r"^if __name__ == \"__main__\":", file_contents, re.M))


def main(args):
    cwd = Path.cwd()
    rglob = list(cwd.rglob("*"))
    bad_files = []
    for file in args.files:
        absolute_path = next((p for p in rglob if str(p).endswith(file)), None)
        if not absolute_path:
            raise ValueError(f"File {file} not found ({absolute_path}).")
        print(f"Checking file {file}...")
        with open(absolute_path, "r") as f:
            if not check_file(f.read()):
                print(f"File {file} is missing the pytest snippet.")
                bad_files.append(file)
    if bad_files:
        raise RuntimeError(f"Found files without pytest snippet: {bad_files}")


parser = argparse.ArgumentParser(
    description='Check if files have the pytest snippet (`if __name__ == "__main__":`)'
)
parser.add_argument("files", nargs="+", help="Files to check")

if __name__ == "__main__":
    args = parser.parse_args()
    main(args)
