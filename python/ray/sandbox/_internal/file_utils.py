import base64
import io
import os
import tarfile
from typing import Union


def encode_file_for_write(path: str, content: Union[str, bytes]) -> str:
    """Generate shell command string to write content into a target file path using base64.

    Args:
        path: Target absolute or relative file path in container.
        content: Text string or binary content.

    Returns:
        Shell command string.
    """
    if isinstance(content, str):
        raw_bytes = content.encode("utf-8")
    else:
        raw_bytes = content

    b64_encoded = base64.b64encode(raw_bytes).decode("ascii")
    parent_dir = os.path.dirname(path)

    mkdir_cmd = f"mkdir -p {parent_dir} && " if parent_dir else ""
    return f"{mkdir_cmd}echo '{b64_encoded}' | base64 -d > '{path}'"


def create_tar_archive(path_in_container: str, content: Union[str, bytes]) -> bytes:
    """Pack file content into a tar archive bytes buffer for tar extraction.

    Args:
        path_in_container: Target path inside container.
        content: File content bytes or str.

    Returns:
        Tar archive bytes buffer.
    """
    if isinstance(content, str):
        raw_bytes = content.encode("utf-8")
    else:
        raw_bytes = content

    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w") as tar:
        tarinfo = tarfile.TarInfo(name=path_in_container.lstrip("/"))
        tarinfo.size = len(raw_bytes)
        tar.addfile(tarinfo, io.BytesIO(raw_bytes))
    return buf.getvalue()
