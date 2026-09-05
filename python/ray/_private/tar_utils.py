import os
import shutil
import tarfile


def _is_relative_to(path: str, base: str) -> bool:
    try:
        return os.path.commonpath([base, path]) == base
    except ValueError:
        return False


def _validate_tar_member(member: tarfile.TarInfo, target_dir: str) -> None:
    if member.isdev() or member.isfifo():
        raise RuntimeError(f"Tarfile member {member.name} is a special file")

    member_path = os.path.abspath(os.path.join(target_dir, member.name))
    if not _is_relative_to(member_path, target_dir):
        raise RuntimeError(f"Tarfile member {member.name} attempts path traversal")

    if member.issym() or member.islnk():
        raise RuntimeError(f"Tarfile member {member.name} is a link")


def _ensure_no_symlink_parent(path: str, target_dir: str) -> None:
    current = target_dir
    rel_path = os.path.relpath(path, target_dir)
    if rel_path == ".":
        return

    for part in rel_path.split(os.sep)[:-1]:
        current = os.path.join(current, part)
        if os.path.islink(current):
            raise RuntimeError(
                f"Tarfile member {rel_path} would extract through a symlink"
            )


def safe_extract_tar(tar: tarfile.TarFile, target_dir: str = ".") -> None:
    """Extract a tarfile while preventing path traversal.

    Python 3.12's ``data`` filter is preferred when available. The fallback keeps
    the same security invariants for older Python versions.
    """
    if hasattr(tarfile, "data_filter"):
        tar.extractall(target_dir, filter="data")
        return

    abs_target = os.path.abspath(target_dir)
    os.makedirs(abs_target, exist_ok=True)
    dirs = []
    for member in tar.getmembers():
        _validate_tar_member(member, abs_target)
        member_path = os.path.abspath(os.path.join(abs_target, member.name))
        _ensure_no_symlink_parent(member_path, abs_target)

        if member.isdir():
            os.makedirs(member_path, exist_ok=True)
            dirs.append((member_path, member.mode, member.mtime))
        elif member.isfile():
            os.makedirs(os.path.dirname(member_path), exist_ok=True)
            source = tar.extractfile(member)
            if source is None:
                raise RuntimeError(f"Tarfile member {member.name} cannot be read")
            with source, open(member_path, "wb") as target:
                shutil.copyfileobj(source, target)
            os.chmod(member_path, member.mode)
            os.utime(member_path, (member.mtime, member.mtime))
        else:
            raise RuntimeError(f"Tarfile member {member.name} has unsupported type")

    for dir_path, mode, mtime in sorted(dirs, reverse=True):
        os.chmod(dir_path, mode)
        os.utime(dir_path, (mtime, mtime))
