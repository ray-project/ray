import os
import shutil
import subprocess
from typing import List, Optional

import runfiles


def gen_extract(
    zip_files: List[str],
    clear_dir_first: Optional[List[str]] = None,
    sub_dir: str = "python",
):
    r = runfiles.Create()
    _repo_name = "io_ray"

    root_dir = os.environ.get("BUILD_WORKSPACE_DIRECTORY")
    if not root_dir:
        raise ValueError(
            "BUILD_WORKSPACE_DIRECTORY not set; please run this script from 'bazelisk run'"
        )

    if sub_dir:
        extract_dir = os.path.join(root_dir, sub_dir)
    else:
        extract_dir = root_dir

    if clear_dir_first:
        for d in clear_dir_first:
            shutil.rmtree(os.path.join(extract_dir, d), ignore_errors=True)

    for zip_file in zip_files:
        zip_path = r.Rlocation(_repo_name + "/" + zip_file)
        if not zip_path:
            raise ValueError(f"Zip file {zip_file} not found")

        # Uses unzip; python zipfile does not restore the file permissions correctly.
        subprocess.check_call(["unzip", "-q", "-o", zip_path, "-d", extract_dir])
