"""Importable filesystem helpers used by distributed file-reader tests."""

import os

from fsspec.implementations.local import LocalFileSystem

import ray
from ray.exceptions import GetTimeoutError


def _wait_for_signal(signal, message):
    try:
        ray.get(signal.wait.remote(), timeout=30)
    except GetTimeoutError as error:
        # fsspec walks omit directories on OSError, including TimeoutError.
        raise AssertionError(message) from error


class UnknownSizeLocalFileSystem(LocalFileSystem):
    """Read real local files while reporting their sizes as unknown."""

    def info(self, path, **kwargs):
        return {**super().info(path, **kwargs), "size": None}

    def ls(self, path, detail=True, **kwargs):
        infos = super().ls(path, detail=True, **kwargs)
        return (
            [{**info, "size": None} for info in infos]
            if detail
            else [info["name"] for info in infos]
        )


class ReadGatedLocalFileSystem(UnknownSizeLocalFileSystem):
    """Hold worker listing of ``later`` until a read task opens a file."""

    def __init__(self, listing_blocked, first_read, **kwargs):
        super().__init__(**kwargs)
        self.listing_blocked = listing_blocked
        self.first_read = first_read

    def ls(self, path, detail=True, **kwargs):
        if (
            os.path.basename(path) == "later"
            and ray.get_runtime_context().get_task_id() is not None
        ):
            ray.get(self.listing_blocked.send.remote())
            _wait_for_signal(self.first_read, "No read task released the listing gate")
        return super().ls(path, detail=detail, **kwargs)

    def _open(self, path, mode="rb", block_size=None, **kwargs):
        _wait_for_signal(
            self.listing_blocked, "Listing never reached the blocking gate"
        )
        stream = super()._open(path, mode, block_size=block_size, **kwargs)
        try:
            ray.get(self.first_read.send.remote())
        except Exception:
            stream.close()
            raise
        return stream
