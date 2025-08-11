from typing import List, Optional
import os
import shutil
import subprocess

import runfiles


def gen_extract(zip_files: List[str], clear_dir_first: Optional[List[str]] = None):
    r = runfiles.Create()
    _repo_name = "com_github_ray_project_ray"

    root_dir = os.environ.get("BUILD_WORKSPACE_DIRECTORY")
    if not root_dir:
        raise ValueError("BUILD_WORKSPACE_DIRECTORY not set")
    python_dir = os.path.join(root_dir, "python")

    if clear_dir_first:
        for d in clear_dir_first:
            shutil.rmtree(os.path.join(python_dir, d), ignore_errors=True)

    for zip_file in zip_files:
        zip_path = r.Rlocation(_repo_name + "/" + zip_file)
        if not zip_path:
            raise ValueError(f"Zip file {zip_file} not found")

        # Uses unzip; python zipfile does not restore the file permissions correctly.
        subprocess.check_call(["unzip", "-q", "-o", zip_path, "-d", python_dir])
