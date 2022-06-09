import os
from pathlib import Path
from typing import Dict


def dir_contents_to_dict(dir: str) -> Dict:
    """Reads all files in a directory (recursively) and returns a dict of binary str.

    Args:
        dir: The directory to read in and return as a dict.

    Returns:
        A dictionary mapping relative path/filenames (relative to given `dir`) to
        binary strings representing the respective files' contents. Note that the
        resulting dir is always "flat" meaning there is only one level of keys
        and all values are (binary?) file contents.
    """
    dir_dict = {}

    # Walk the tree.
    for root, directories, files in os.walk(dir):
        for filename in files:
            # Join the two strings in order to form the full filepath.
            filepath = os.path.join(root, filename)
            with open(filepath, mode="rb") as file:
                dir_dict[os.path.join(root[len(dir) + 1 :], filename)] = file.read()

    return dir_dict


def dict_contents_to_dir(dir_dict: Dict, base_dir: str) -> None:
    """

    Args:
        dir_dict:
    """
    for sub_dir, file_content in dir_dict.items():
        dirname = Path(os.path.join(base_dir, Path(sub_dir).parent))
        dirname.mkdir(parents=True, exist_ok=True)
        with open(os.path.join(base_dir, sub_dir), mode="wb") as file:
            file.write(file_content)
