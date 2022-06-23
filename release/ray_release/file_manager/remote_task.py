import io
import tarfile
from typing import Optional

from ray_release.file_manager.file_manager import FileManager


def _pack(source_dir: str) -> bytes:
    stream = io.BytesIO()
    with tarfile.open(fileobj=stream, mode="w:gz", format=tarfile.PAX_FORMAT) as tar:
        tar.add(source_dir, arcname="")

    return stream.getvalue()


def _unpack(stream: bytes, target_dir: str):
    with tarfile.open(fileobj=io.BytesIO(stream)) as tar:
        tar.extractall(target_dir)


def send_dir_to_node(
    node_ip: str,
    local_dir: str,
    remote_dir: str,
):
    import ray

    try:
        packed = _pack(local_dir)
        ray.get(
            ray.remote(resources={f"node:{node_ip}": 0.01})(_unpack).remote(
                packed, remote_dir
            )
        )
    except Exception as e:
        print(
            f"Warning: Could not send remote directory contents. Message: " f"{str(e)}"
        )


def fetch_dir_from_node(
    node_ip: str,
    remote_dir: str,
    local_dir: str,
):
    import ray

    try:
        packed = ray.get(
            ray.remote(resources={f"node:{node_ip}": 0.01})(_pack).remote(remote_dir)
        )
        _unpack(packed, local_dir)
    except Exception as e:
        print(f"Warning: Could not fetch remote directory contents. Message: {str(e)}")


def _get_head_ip():
    import ray

    return ray.util.get_node_ip_address()


def send_dir_to_head(local_dir: str, remote_dir: str):
    import ray

    ip = ray.get(ray.remote(_get_head_ip).remote())
    return send_dir_to_node(ip, local_dir, remote_dir)


def fetch_dir_fom_head(local_dir: str, remote_dir: str):
    import ray

    ip = ray.get(ray.remote(_get_head_ip).remote())
    return fetch_dir_from_node(ip, remote_dir, local_dir)


class RemoteTaskFileManager(FileManager):
    def upload(self, source: Optional[str] = None, target: Optional[str] = None):
        send_dir_to_head(source, target)

    def download(self, source: str, target: str):
        fetch_dir_fom_head(source, target)
