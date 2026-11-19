import os
import shutil
import stat
import subprocess
import sys
import zipfile
from typing import Callable, List, Optional

import runfiles


def _remove_readonly(func: Callable[[str], None], path: str, exc_info: tuple) -> None:
    error = exc_info[1]
    if isinstance(error, FileNotFoundError):
        return
    if not isinstance(error, PermissionError) or func not in (os.unlink, os.rmdir):
        raise error
    attrs = os.lstat(path).st_file_attributes
    # Never follow a link or junction to change its target's permissions.
    if (
        not attrs & stat.FILE_ATTRIBUTE_READONLY
        or attrs & stat.FILE_ATTRIBUTE_REPARSE_POINT
    ):
        raise error
    os.chmod(path, stat.S_IWRITE)
    func(path)


def _extract_windows_zip(zip_path: str, extract_dir: str) -> None:
    with zipfile.ZipFile(zip_path) as archive:
        for member in archive.infolist():
            try:
                archive.extract(member, extract_dir)
            except PermissionError as exc:
                # Retry a read-only build output using zipfile's normalized path.
                target = exc.filename
                if (
                    not target
                    or not os.path.isfile(target)
                    or os.access(target, os.W_OK)
                ):
                    raise
                os.chmod(target, stat.S_IWRITE)
                archive.extract(member, extract_dir)


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
            path = os.path.join(extract_dir, d)
            if sys.platform == "win32":
                shutil.rmtree(path, onerror=_remove_readonly)
            else:
                shutil.rmtree(path, ignore_errors=True)

    for zip_file in zip_files:
        zip_path = r.Rlocation(_repo_name + "/" + zip_file)
        if not zip_path:
            raise ValueError(f"Zip file {zip_file} not found")

        if sys.platform == "win32":
            _extract_windows_zip(zip_path, extract_dir)
        else:
            # zipfile does not restore the executable permissions needed on POSIX.
            subprocess.check_call(["unzip", "-q", "-o", zip_path, "-d", extract_dir])
