#!/usr/bin/env python3
import os
import re
from fsspec import filesystem

def find_files(directory):
    """Recursively find all files in a given directory, returning just filenames with extension."""
    all_files = []
    for root, _, files in os.walk(directory):
        for file in files:
            all_files.append(file)  # Only include the filename
    return all_files

def find_meson_build_files(directory):
    """Find all meson.build files including one outside the specified directory."""
    meson_files = []
    for root, _, files in os.walk(directory):
        for file in files:
            if file == 'meson.build':
                meson_files.append(os.path.join(root, file))

    # Check for meson.build in the parent directory
    parent_dir = os.path.dirname(directory)
    parent_meson_file = os.path.join(parent_dir, 'meson.build')
    if os.path.isfile(parent_meson_file):
        meson_files.append(parent_meson_file)

    return meson_files

def extract_references(meson_files):
    """Extract file references from meson.build files, returning just filenames."""
    references = set()
    for meson_file in meson_files:
        with open(meson_file, 'r') as f:
            content = f.read()
            # Assuming references are file paths or patterns
            matches = re.findall(r'"([^"]+)"', content)
            # Extract just the filename from the full path
            filenames = [os.path.basename(match) for match in matches]
            references.update(filenames)
    return references

def exclude_files(files, exclude_patterns):
    """Exclude files matching the given list of regex patterns."""
    excluded_files = files[:]
    for pattern in exclude_patterns:
        excluded_files = [f for f in excluded_files if not re.search(pattern, f)]
    return excluded_files


def main(root_directory, exclude_patterns):
    all_files = find_files(root_directory)
    meson_files = find_meson_build_files(root_directory)

    referenced_files = extract_references(meson_files)

    # Exclude specified files
    excluded_files = exclude_files(all_files, exclude_patterns)

    # Identify unused files
    unused_files = [f for f in excluded_files if f not in referenced_files]

    print(f"Unused Files: {len(unused_files)}")
    for unused in unused_files:
        print(unused)


# Example usage with multiple regex patterns
if __name__ == "__main__":
    root_dir = "src"  # Replace with your directory path
    local_fs = filesystem("dir", path=root_dir)

    exclude_regex_patterns = [r'\.tmp$', r'\.log$', r'test\.cc$']  # List of regex patterns to exclude

    main(root_dir, exclude_regex_patterns)
