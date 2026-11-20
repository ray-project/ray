"""Node-wide, content-addressed cache of verified container image layers."""

import concurrent.futures
import fcntl
import hashlib
import logging
import os
import re
import tempfile
import threading
import time
import urllib.request
from typing import BinaryIO, Iterator, Tuple

from ray.experimental.sandbox.exceptions import SandboxCreationError

logger = logging.getLogger(__name__)

# Image cache names strip leading dots, keeping this namespace reserved.
BLOBS_SUBDIR = ".blobs"
_DIGEST = re.compile(r"sha256:([0-9a-f]{64})")
_CHUNK = 1024 * 1024


def _blob_dir(images_dir: str) -> str:
    return os.path.join(images_dir, BLOBS_SUBDIR, "sha256")


def blob_path(images_dir: str, digest: str) -> str:
    """Return the cached path for a SHA-256 layer digest."""
    match = _DIGEST.fullmatch(digest)
    if not match:
        raise SandboxCreationError(f"Unsupported layer digest {digest!r}")
    return os.path.join(_blob_dir(images_dir), match.group(1))


def _download(
    request: urllib.request.Request,
    digest: str,
    path: str,
    timeout: float,
    stop: threading.Event,
) -> None:
    """Verify a temporary download before publishing it under its digest."""
    with tempfile.NamedTemporaryFile(
        dir=os.path.dirname(path),
        prefix=f".partial-{os.path.basename(path)}-",
        delete=False,
    ) as partial:
        try:
            hasher = hashlib.sha256()
            with urllib.request.urlopen(request, timeout=timeout) as response:
                while not stop.is_set():
                    chunk = response.read1(_CHUNK)
                    if not chunk:
                        break
                    hasher.update(chunk)
                    partial.write(chunk)
            if stop.is_set():
                raise concurrent.futures.CancelledError()
            if hasher.hexdigest() != digest.removeprefix("sha256:"):
                raise SandboxCreationError(
                    f"Layer {digest} failed verification: got sha256:{hasher.hexdigest()}"
                )
            partial.flush()
            os.fchmod(partial.fileno(), 0o644)
            os.replace(partial.name, path)
        except BaseException:
            try:
                os.unlink(partial.name)
            except OSError as error:
                logger.warning(
                    "Failed to remove incomplete sandbox layer %s: %s",
                    partial.name,
                    error,
                )
            raise


def fetch_blob(
    images_dir: str,
    digest: str,
    request: urllib.request.Request,
    timeout: float,
    stop: threading.Event,
) -> BinaryIO:
    """Open a cached layer, downloading it once under a cross-process lock.

    Open the file before releasing the lock so it remains readable even if
    another pull evicts its directory entry before this image applies it.
    """
    path = blob_path(images_dir, digest)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(f"{path}.lock", "a", encoding="utf-8") as lock:
        while True:
            if stop.is_set():
                raise concurrent.futures.CancelledError()
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except BlockingIOError:
                stop.wait(0.05)
        if os.path.isfile(path):
            try:
                os.utime(path)
            except OSError as error:
                logger.warning(
                    "Failed to update last-used time for cached sandbox layer %s: %s",
                    digest,
                    error,
                )
        else:
            _download(request, digest, path, timeout, stop)
            logger.info("Cached sandbox image layer %s", digest)
        return open(path, "rb")


def _remove_stale_partial(images_dir: str, name: str, now: float) -> None:
    """Remove an abandoned partial download only when its layer is unlocked."""
    path = os.path.join(_blob_dir(images_dir), name)
    try:
        # Only consider incomplete downloads abandoned after more than one hour.
        if now - os.stat(path).st_mtime <= 3600:
            return
        digest = "sha256:" + name.removeprefix(".partial-").split("-", 1)[0]
        if not _DIGEST.fullmatch(digest):
            return
        lock_path = f"{blob_path(images_dir, digest)}.lock"
        with open(lock_path, "a", encoding="utf-8") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            os.unlink(path)
    except (BlockingIOError, FileNotFoundError):
        pass
    except OSError as error:
        logger.warning("Failed to remove stale sandbox layer %s: %s", path, error)


def cached_blobs(images_dir: str) -> Iterator[Tuple[float, str, int]]:
    """Yield each blob's mtime, relative path and size for image-cache eviction."""
    directory = _blob_dir(images_dir)
    try:
        names = os.listdir(directory)
    except FileNotFoundError:
        return
    except OSError as error:
        logger.warning(
            "Failed to scan cached sandbox layers in %s: %s", directory, error
        )
        return
    now = time.time()
    for name in names:
        path = os.path.join(directory, name)
        if name.startswith(".partial-"):
            _remove_stale_partial(images_dir, name, now)
            continue
        if not re.fullmatch(r"[0-9a-f]{64}", name):
            continue
        try:
            stat = os.stat(path)
        except FileNotFoundError:
            continue
        except OSError as error:
            logger.warning("Failed to inspect cached sandbox layer %s: %s", path, error)
            continue
        yield stat.st_mtime, os.path.relpath(path, images_dir), stat.st_size
