#!/usr/bin/env python3
# MARK: - 1. Imports
# MARK: - 1.1. stdlib
from functools import cached_property
from pathlib import Path
import re
from pprint import pprint
from dataclasses import dataclass

# MARK: - 1.2. 3rd-party
from fsspec import filesystem

@dataclass(frozen=True, eq=True)
class NativeFile:
    path: Path

    @cached_property
    def name(self) -> str:
        return self.path.name


local_fs = filesystem("dir", path=Path(__file__).parent)

# ".h" - they aren't mentioned explicitly but required by .cc and .c files, so no need to check
NATIVE_FILE_EXTENSIONS = frozenset([".c", ".cc"])

def find_native_source_files(root_dir: Path) -> frozenset[NativeFile]:
    native_source_files: set[str] = set()

    for extension in NATIVE_FILE_EXTENSIONS:
        native_source_files.update(local_fs.glob(f"{str(root_dir)}/**/*{extension}"))

    return frozenset(map(lambda _0: NativeFile(path=Path(_0)), native_source_files))


def find_meson_build_files(root_dir: Path) -> frozenset[Path]:
    return frozenset(map(Path, local_fs.glob(f"{str(root_dir)}/**/meson.build")))


def filter_files(
    files: frozenset[NativeFile], /,
    exclude_patterns: frozenset[str]
) -> frozenset[NativeFile]:
    excluded_files: set[NativeFile] = set()

    for file in files:
        for pattern in exclude_patterns:
            if re.search(pattern, file.name) is not None or re.search(pattern, str(file.path)) is not None:
                excluded_files.add(file)

    return files.difference(excluded_files)


def does_file_contains_string(file_path: Path, search_string: str):
    # Escape the search string to handle any special characters
    escaped_string = re.escape(search_string)
    # Create a regex pattern to match the desired single-quoted string
    pattern = rf"({escaped_string})"

    with open(file_path, 'r') as file:
        content = file.read()
        # Use re.search to find the pattern in the content
        return re.search(pattern, content) is not None


def main(root_dir: Path, exclude_patterns: frozenset[str]):
    meson_build_file_paths = find_meson_build_files(root_dir=root_dir)

    native_source_files = filter_files(
        find_native_source_files(root_dir=root_dir / "src"),
        exclude_patterns=exclude_patterns
    )

    used_native_source_files: set[NativeFile] = set()
    for native_source_file in native_source_files:
        for meson_build_file_path in meson_build_file_paths:
            if does_file_contains_string(meson_build_file_path, native_source_file.name):
                used_native_source_files.add(native_source_file)

    unused_native_source_files = native_source_files.difference(used_native_source_files)
    unused_native_source_files_tuples = list(sorted(map(lambda _0: (str(_0.path), _0.name), unused_native_source_files), key=lambda _0: _0[0]))
    if len(unused_native_source_files_tuples) == 0:
        print("No raptors found! Great job.")
    else:
        for unused_native_source_file_tuple in unused_native_source_files_tuples:
            print(*unused_native_source_file_tuple)


# Example usage with multiple regex patterns
if __name__ == "__main__":
    main(
        root_dir=Path("./"),
        exclude_patterns=frozenset([r'(?i)(?:^|/)(java|test)(?:/|$)', r'test\.cc$'])
    )
