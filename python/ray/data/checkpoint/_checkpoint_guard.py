"""Detect overlapping checkpoint writers within one Ray cluster."""

import hashlib
import json
import os
import threading
import uuid

from pyarrow.fs import FileSystem, LocalFileSystem

import ray
from ray.data.datasource.path_util import _unwrap_protocol
from ray.experimental.internal_kv import (
    _internal_kv_del,
    _internal_kv_get,
    _internal_kv_put,
)

_NAMESPACE = "ray.data.checkpoint.guard"
_CREATE_LOCK = threading.Lock()


def _checkpoint_key(checkpoint_path, filesystem) -> bytes:
    path = filesystem.normalize_path(_unwrap_protocol(checkpoint_path))
    if isinstance(filesystem, LocalFileSystem):
        if checkpoint_path.startswith("file://"):
            _, path = FileSystem.from_uri(checkpoint_path)
        path = os.path.normcase(os.path.realpath(path))
    else:
        # Object keys containing internal '/' or '..' components aren't POSIX
        # paths. Only remove the directory's trailing separator.
        path = path.rstrip("/")
    identity = f"{filesystem.type_name}:{path}"
    return hashlib.sha256(identity.encode("utf-8")).hexdigest().encode("ascii")


class _CheckpointGuardCoordinator:
    """Serialize ownership changes; keep ownership in GCS across actor restarts."""

    def acquire(self, key: bytes, token: str, job_id: str) -> bool:
        existing = _internal_kv_get(key, namespace=_NAMESPACE)
        if existing is not None:
            owner = json.loads(existing)
            if owner == [token, job_id]:
                return True
            # Unknown jobs aren't proof of termination. Never expire a live
            # owner's guard based on elapsed time or a slow driver.
            owner_finished = any(
                job["JobID"] == owner[1] and job["IsDead"]
                for job in ray._private.state.jobs()
            )
            if not owner_finished:
                raise RuntimeError(
                    "The checkpoint path is already in use by Ray job "
                    f"{owner[1]}. Wait for that write to finish or use a different "
                    "checkpoint path."
                )
            _internal_kv_del(key, namespace=_NAMESPACE)
        return not (
            _internal_kv_put(
                key,
                json.dumps([token, job_id]).encode("utf-8"),
                overwrite=False,
                namespace=_NAMESPACE,
            )
        )

    def release(self, key: bytes, token: str, job_id: str) -> None:
        existing = _internal_kv_get(key, namespace=_NAMESPACE)
        if existing is not None and json.loads(existing) == [token, job_id]:
            _internal_kv_del(key, namespace=_NAMESPACE)


def _get_coordinator():
    # Ray's get_if_exists registration is atomic across drivers. The local lock
    # also supports concurrent callers within a driver.
    with _CREATE_LOCK:
        return (
            ray.remote(_CheckpointGuardCoordinator)
            .options(
                name="CheckpointGuardCoordinator",
                namespace=_NAMESPACE,
                get_if_exists=True,
                lifetime="detached",
                num_cpus=0,
                max_restarts=-1,
                max_task_retries=-1,
            )
            .remote()
        )


class CheckpointPathGuard:
    """Hold a cooperative checkpoint-path guard through driver finalization.

    This guards jobs in all namespaces of the same cluster. It isn't a storage
    lock: separate clusters and alternate filesystem aliases still require
    external coordination. Ownership doesn't expire while the job is alive.
    """

    def __init__(self, checkpoint_path, filesystem):
        self._key = _checkpoint_key(checkpoint_path, filesystem)
        self._token = uuid.uuid4().hex
        self._job_id = None
        self._coordinator = None

    def acquire(self) -> None:
        if self._coordinator is None:
            self._job_id = ray.get_runtime_context().get_job_id()
            self._coordinator = _get_coordinator()
        if not ray.get(
            self._coordinator.acquire.remote(self._key, self._token, self._job_id)
        ):
            raise RuntimeError("The checkpoint path was acquired by another writer.")

    def release(self) -> None:
        if self._coordinator is not None:
            ray.get(
                self._coordinator.release.remote(self._key, self._token, self._job_id)
            )
            self._coordinator = None
            self._token = uuid.uuid4().hex
