import tarfile
import tempfile
from typing import Optional

from ray_release.uploader.uploader import Uploader


def _pack(source_dir: str) -> bytes:
    tmpfile = tempfile.mktemp()
    with tarfile.open(tmpfile, "w:gz") as tar:
        tar.add(source_dir, arcname="")

    with open(tmpfile, "rb") as f:
        stream = f.read()

    return stream


def _unpack(stream: bytes, target_dir: str):
    tmpfile = tempfile.mktemp()

    with open(tmpfile, "wb") as f:
        f.write(stream)

    with tarfile.open(tmpfile) as tar:
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
                packed, remote_dir))
    except Exception as e:
        print(f"Warning: Could not send remote directory contents. Message: "
              f"{str(e)}")


def fetch_dir_from_node(
        node_ip: str,
        remote_dir: str,
        local_dir: str,
):
    import ray

    try:
        packed = ray.get(
            ray.remote(
                resources={f"node:{node_ip}": 0.01})(_pack).remote(remote_dir))
        _unpack(packed, local_dir)
    except Exception as e:
        print(f"Warning: Could not fetch remote directory contents. Message: "
              f"{str(e)}")


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


class RuntimeEnvUploader(Uploader):
    def upload(self, source_dir: str, target_dir: Optional[str] = None):
        target_dir = self.get_target_dir(source_dir, target_dir)
        send_dir_to_head(source_dir, target_dir)
