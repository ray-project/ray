"""Builds images from :class:`~ray.experimental.sandbox.image_build.ImageBuildPlan`\\ s.

Each step of a plan becomes a cached layer in the node's image cache, a
top-level entry ``raylayer_<key>`` next to the pulled images:

* ``layer.tar``: the step's changes, as an OCI layer.
* ``layer.json``: what the step was, its parent, and which build of it this is.
* ``rootfs.erofs`` and ``.image_config.json``: only for an image a sandbox
  actually runs (a *leaf*), flattened from the base and every layer.

A layer's key hashes its parent's key, its parent's force epoch and its own
definition, so images that share a prefix of steps share those layers, and a
sandbox on a cached image starts after a few ``stat`` calls.

Building a layer runs its step in a short-lived *build sandbox* on the image
so far and exports what changed with ``runsc tar rootfs-upper``. Cached and
COPY layers need no sandbox. Each layer is appended to a private work copy of
the image with ``mkfs.erofs --incremental``, and the finished copy becomes the
leaf's ``rootfs.erofs``.

Only running sandboxes pin entries, and only the leaf they mount: a leaf is
self-contained, so its base and intermediate layers may be evicted while it
runs, at the cost of rebuilding them when another image needs them.

Locking: each entry has a build lock and a short lock. A build holds its
leaf's build lock throughout, and at most one ancestor layer's at a time,
only while producing that layer. A builder therefore only ever waits, holding
nothing but its own leaf, for a strictly shallower entry, so no cycle can
form. Pins and eviction take the short lock, which a build holds only to swap
a finished entry in, so a ready image never waits for a build of itself.

A build that dies leaves its work copy and its pin on the image it started
from; the build's locked owner file tells a later sweep so (see
``_internal/image_layers.sweep_stale_work_dirs``). Build sandboxes are
ordinary sandboxes: the backend reclaims them as it does any sandbox whose
owner died.
"""

import contextlib
import datetime
import fcntl
import hashlib
import json
import logging
import os
import re
import shutil
import subprocess
import threading
import time
import uuid
from collections import deque
from typing import BinaryIO, Callable, Deque, Dict, List, Optional, Set, Tuple

from ray.experimental.sandbox._internal.erofs_reader import ErofsError, ErofsImage
from ray.experimental.sandbox._internal.image_layers import (
    LayerInfo,
    apply_layer_to_erofs,
    archive_path_from_image,
    base_build_id,
    build_token,
    claim_epoch,
    layer_reference,
    load_build_env_key,
    normalize_overlay_diff,
    own_work_dir,
    prepare_copy_layer,
    read_epoch,
    read_layer_json,
    require_incremental_mkfs_erofs,
    sweep_stale_work_dirs,
    verify_erofs_image,
    work_reference,
    write_layer_json,
)
from ray.experimental.sandbox._internal.image_utils import (
    _USERS_SUBDIR,
    LAYER_TAR,
    ROOTFS_IMAGE,
    BuiltImageEvictedError,
    _mark_image_in_use,
    entry_build_lock_path,
    entry_lock_path,
    evict_to_cache_limits,
    expected_extract_marker,
    get_platform_arch,
    hold_lock,
    sanitize_image_name,
)
from ray.experimental.sandbox.backend.base import BaseSandboxBackend
from ray.experimental.sandbox.config import (
    DOCKER_DEFAULT_CAPABILITIES,
    SandboxConfig,
    parse_memory_bytes,
)
from ray.experimental.sandbox.exceptions import (
    SandboxCreationError,
    SandboxImageBuildError,
)
from ray.experimental.sandbox.image_build import (
    BuildStep,
    CopyFromImageStep,
    CopyStep,
    ImageBuildPlan,
    RunStep,
    base_key,
    step_key,
)
from ray.experimental.sandbox.image_manager import ImageManager
from ray.util.annotations import DeveloperAPI

logger = logging.getLogger(__name__)

_BUILD_TIMEOUT_ENV = "RAY_SANDBOX_IMAGE_BUILD_TIMEOUT_SECONDS"
_DEFAULT_STEP_TIMEOUT = 3600.0
# How many images a node builds at once; 0 for no limit.
_CONCURRENCY_ENV = "RAY_SANDBOX_IMAGE_BUILD_CONCURRENCY"
_DEFAULT_CONCURRENCY = 2
_SLOTS_DIR = ".build-slots"
# Caps for each build sandbox; unset, a build may use the whole node.
_BUILD_CPU_ENV = "RAY_SANDBOX_IMAGE_BUILD_CPU"
_BUILD_MEMORY_ENV = "RAY_SANDBOX_IMAGE_BUILD_MEMORY"
# The network build sandboxes get: "public", as Modal's builds have, or
# "none" for nodes that must not reach out while building.
_BUILD_NETWORK_ENV = "RAY_SANDBOX_IMAGE_BUILD_NETWORK"
_BUILD_NETWORKS = ("public", "none")
# Set to 0 to publish built images without checking them with fsck.erofs.
_VERIFY_ENV = "RAY_SANDBOX_IMAGE_BUILD_VERIFY"
# A hit refreshes the LRU time of the layers below the image at most this
# often, rather than writing to every one of them on every create.
_ANCESTOR_TOUCH_SECONDS = 60.0
# How often one build may start over to use a layer another build just made,
# before it builds that layer itself.
_MAX_REBASES = 3
# How much of a failed step's output its error message carries.
_OUTPUT_TAIL_BYTES = 64 * 1024
# How long to keep collecting a command's output once it has exited.
_OUTPUT_DRAIN_SECONDS = 5.0
_MARKER = ".extracted"
_IMAGE_CONFIG = ".image_config.json"
# How often a process looks for the work copies of builds that died, per
# image cache.
_SWEEP_INTERVAL_SECONDS = 60.0
_sweep_lock = threading.Lock()
_last_sweep: Dict[str, float] = {}


def _step_timeout() -> float:
    raw = os.environ.get(_BUILD_TIMEOUT_ENV)
    if raw:
        try:
            value = float(raw)
            if value > 0:
                return value
        except ValueError:
            logger.warning("Ignoring %s=%r: expected seconds.", _BUILD_TIMEOUT_ENV, raw)
    return _DEFAULT_STEP_TIMEOUT


def _build_concurrency() -> int:
    raw = os.environ.get(_CONCURRENCY_ENV)
    if raw:
        try:
            return max(int(raw), 0)
        except ValueError:
            logger.warning(
                "Ignoring %s=%r: expected an integer.", _CONCURRENCY_ENV, raw
            )
    return _DEFAULT_CONCURRENCY


def _build_sandbox_settings() -> Dict:
    """``SandboxConfig`` fields for build sandboxes, from the environment.

    Their network, and their caps. Deliberately not the sandbox's own
    ``cpu``/``memory``: a small sandbox on a big image would then fail a
    build -- ``pip install torch`` in 512 MiB -- that Modal, which builds with
    its own resources, runs fine.
    """
    settings: Dict = {"network": "public"}
    network = os.environ.get(_BUILD_NETWORK_ENV)
    if network:
        if network in _BUILD_NETWORKS:
            settings["network"] = network
        else:
            logger.warning(
                "Ignoring %s=%r: expected one of %s.",
                _BUILD_NETWORK_ENV,
                network,
                ", ".join(_BUILD_NETWORKS),
            )
    cpu = os.environ.get(_BUILD_CPU_ENV)
    if cpu:
        try:
            settings["cpu"] = float(cpu)
        except ValueError:
            logger.warning(
                "Ignoring %s=%r: expected a number of CPUs.", _BUILD_CPU_ENV, cpu
            )
    memory = os.environ.get(_BUILD_MEMORY_ENV)
    if memory:
        try:
            parse_memory_bytes(memory)
            settings["memory"] = memory
        except (TypeError, ValueError):
            logger.warning(
                "Ignoring %s=%r: expected a size such as '8Gi'.",
                _BUILD_MEMORY_ENV,
                memory,
            )
    return settings


def _now_iso() -> str:
    return datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _hash(*parts: str) -> str:
    return "sha256:" + hashlib.sha256("\0".join(parts).encode("utf-8")).hexdigest()


class _BuildCancelled(SandboxImageBuildError):
    """The build was cancelled (see :meth:`ImageBuilder.cancel`)."""

    def __init__(
        self,
        message: str = "The image build was cancelled.",
        image_id: Optional[str] = None,
    ):
        super().__init__(message, image_id)


class _BuildRun:
    """One :meth:`ImageBuilder.build` call in progress, for ``cancel`` to reach."""

    def __init__(self):
        self._lock = threading.Lock()
        self._cancelled = False
        # Stops the command the build is running in a build sandbox, if any.
        self._stop: Optional[Callable[[], None]] = None

    def cancel(self) -> None:
        with self._lock:
            self._cancelled = True
            stop = self._stop
        if stop is not None:
            stop()

    def check(self) -> None:
        """Raise ``_BuildCancelled`` if the build was cancelled."""
        if self._cancelled:
            raise _BuildCancelled()

    def running(self, stop: Optional[Callable[[], None]]) -> None:
        """Set what stops the command now running, or None once it ended.

        A cancel that came before the command started stops it at once.
        """
        with self._lock:
            self._stop = stop
            cancelled = self._cancelled
        if stop is not None and cancelled:
            stop()


class _Rebase(Exception):
    """Another build just made a layer this build was about to make.

    It made it on a lineage this build's work copy is not on -- after an
    eviction, say -- so the diff cannot be replayed onto the work copy, but
    the cache now supplies the layer some other way. The build starts over
    from that instead of running the step again and replacing what the other
    build published, which may include its image.

    Args:
        key: The layer's key.
    """

    def __init__(self, key: str):
        super().__init__(key)
        self.key = key


class _Chain:
    """The keys of a plan's layers, base first: ``keys[i]`` is layer i's."""

    def __init__(self, keys: List[str], forced: Set[int]):
        self.keys = keys
        # Indexes (1-based, like keys) of forced layers still to be rebuilt.
        self.forced = forced

    @property
    def leaf(self) -> str:
        return self.keys[-1]


@DeveloperAPI
class ImageBuilder:
    """Builds and caches images on this node.

    Args:
        image_manager: The node's image cache; layers live in its
            ``images_dir``.
        backend: Runs the build sandboxes; must support
            ``export_rootfs_upper``.
    """

    def __init__(self, image_manager: ImageManager, backend: BaseSandboxBackend):
        self._image_manager = image_manager
        self._backend = backend
        # Read on the first plan with build-only environment values.
        self._build_env_key: Optional[bytes] = None
        # The builds in progress, by the thread running each.
        self._runs: Dict[int, _BuildRun] = {}
        self._runs_lock = threading.Lock()

    @property
    def images_dir(self) -> str:
        return self._image_manager.images_dir

    # -- public ------------------------------------------------------------

    def cancel(self) -> None:
        """Cancel the builds this builder is running; callable from any thread.

        Each build stops at its next step, and at once if it is running a
        command (killed inside its build sandbox) or waiting for a build slot
        or for another build of the same image. A build that is exporting or
        applying a layer finishes that first. What a cancelled build made is
        cleaned up as after a failed step -- its build sandboxes deleted, its
        work copy removed, its pins released -- and its :meth:`build` call
        raises ``SandboxImageBuildError``. Layers it already published stay
        cached. Builds that start after this call are not affected.

        May block for a few seconds, killing the commands the builds run.
        """
        with self._runs_lock:
            runs = list(self._runs.values())
        for run in runs:
            run.cancel()

    def _check_cancelled(self) -> None:
        """Raise ``_BuildCancelled`` if the calling thread's build was cancelled."""
        run = self._runs.get(threading.get_ident())
        if run is not None:
            run.check()

    def build(
        self,
        base: str,
        plan: ImageBuildPlan,
        *,
        timeout_seconds: float = 120.0,
        base_force_token: Optional[str] = None,
        rootless: bool = True,
        ignore_cgroups: bool = False,
    ) -> str:
        """Make sure the image ``base`` + ``plan`` is cached, building what is missing.

        Args:
            base: Base image reference or local tar path.
            plan: The steps on top of it.
            timeout_seconds: Network timeout for pulls, and how long a build
                sandbox may take to start. Each step's commands get
                ``RAY_SANDBOX_IMAGE_BUILD_TIMEOUT_SECONDS`` (default an hour).
            base_force_token: Set when the base was just force-pulled for this
                token; layers built on the old base are then re-keyed once.
            rootless: Passed to the build sandboxes.
            ignore_cgroups: Passed to the build sandboxes.

        Returns:
            The image's reference, for ``SandboxConfig.image``.

        Raises:
            SandboxImageBuildError: A step failed, or :meth:`cancel` cancelled
                the build.
            SandboxCreationError: The node cannot build (no runsc, an old
                mkfs.erofs) or the base cannot be pulled.
        """
        run = _BuildRun()
        thread = threading.get_ident()
        with self._runs_lock:
            self._runs[thread] = run
        try:
            return self._build(
                base,
                plan,
                timeout_seconds=timeout_seconds,
                base_force_token=base_force_token,
                rootless=rootless,
                ignore_cgroups=ignore_cgroups,
            )
        finally:
            with self._runs_lock:
                self._runs.pop(thread, None)

    def _build(
        self,
        base: str,
        plan: ImageBuildPlan,
        *,
        timeout_seconds: float,
        base_force_token: Optional[str],
        rootless: bool,
        ignore_cgroups: bool,
    ) -> str:
        self._maybe_sweep()
        if base_force_token is not None:
            key = self._base_key(base)
            with self._lock(key):
                claim_epoch(self.images_dir, key, base_force_token)
        chain = self._resolve_chain(base, plan)
        if self._leaf_ready(chain):
            return self._hit(chain)
        require_incremental_mkfs_erofs()
        # A build of this very image may be under way: wait for it before
        # queueing for a build slot, since it usually leaves the image ready.
        # Nothing else is held while waiting, so this waits on no one who
        # waits on us.
        with self._lock(chain.leaf, waiting="image"):
            pass
        chain = self._resolve_chain(base, plan)
        if self._leaf_ready(chain):
            return self._hit(chain)
        with self._build_slot():
            return self._build_locked(
                base,
                plan,
                chain,
                timeout_seconds=timeout_seconds,
                sandbox_options={
                    "rootless": rootless,
                    "_ignore_cgroups": ignore_cgroups,
                    **_build_sandbox_settings(),
                },
            )

    def _maybe_sweep(self) -> None:
        """Start ``sweep_stale_work_dirs`` in the background, at most once a
        minute per process and image cache.

        A build that died keeps a pin on the image it started from, and with
        it that image out of eviction's reach. Sweeping on every build, cache
        hits included, frees it once any sandbox on the node starts on a built
        image, not only once another build has to run. A build never waits
        for it.
        """
        images_dir = self.images_dir
        with _sweep_lock:
            now = time.monotonic()
            last = _last_sweep.get(images_dir, float("-inf"))
            if now - last < _SWEEP_INTERVAL_SECONDS:
                return
            _last_sweep[images_dir] = now
        threading.Thread(
            target=sweep_stale_work_dirs,
            args=(images_dir,),
            name="sandbox-image-build-sweep",
            daemon=True,
        ).start()

    def _hit(self, chain: _Chain) -> str:
        """Use the cached image of ``chain``, refreshing it and its layers for LRU.

        The layers below it are touched too, though nothing reads them on a
        hit: they are what rebuilds this image once eviction takes its
        flattened copy, and what other images with the same leading steps
        build on, so they should outlive layers that no image in use needs.
        """
        self._touch(chain.leaf)
        now = time.time()
        for key in chain.keys[1:-1]:
            marker = os.path.join(self._entry_dir(key), _MARKER)
            try:
                if now - os.path.getmtime(marker) > _ANCESTOR_TOUCH_SECONDS:
                    os.utime(marker)
            except OSError:
                pass
        return layer_reference(chain.leaf)

    @contextlib.contextmanager
    def _build_slot(self):
        """Hold one of the node's ``RAY_SANDBOX_IMAGE_BUILD_CONCURRENCY`` build slots.

        Every build copies its start image and runs its own build sandboxes,
        so builds of different images at once multiply disk and CPU use;
        the slots bound that. A slot is taken before any entry lock and never
        awaited while holding one, and only slot holders keep entry locks for
        long, so a slot always comes free.
        """
        limit = _build_concurrency()
        if limit == 0:
            yield
            return
        slots = os.path.join(self.images_dir, _SLOTS_DIR)
        os.makedirs(slots, mode=0o777, exist_ok=True)
        delay = 0.05
        waiting_since = None
        while True:
            for index in range(limit):
                f = open(os.path.join(slots, str(index)), "a", encoding="utf-8")
                try:
                    fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
                except OSError:
                    f.close()
                    continue
                if waiting_since is not None:
                    logger.info(
                        "Got an image build slot after %.1fs",
                        time.monotonic() - waiting_since,
                    )
                try:
                    yield
                finally:
                    fcntl.flock(f, fcntl.LOCK_UN)
                    f.close()
                return
            if waiting_since is None:
                waiting_since = time.monotonic()
                logger.info(
                    "Waiting for one of this node's %d image build slots (%s)",
                    limit,
                    _CONCURRENCY_ENV,
                )
            self._check_cancelled()
            time.sleep(delay)
            delay = min(delay * 2, 1.0)

    def _build_locked(
        self,
        base: str,
        plan: ImageBuildPlan,
        chain: _Chain,
        *,
        timeout_seconds: float,
        sandbox_options: Dict,
    ) -> str:
        """Build the image under its leaf's lock, unless it is ready by then."""
        for _ in range(5):
            with self._lock(chain.leaf, waiting="image"):
                # Re-resolved under the lock: a build that held it may have
                # just rebuilt forced layers this chain still counted as
                # pending, and only the leaf lock makes that visible.
                current = self._resolve_chain(base, plan, held=chain.leaf)
                if current.leaf != chain.leaf:
                    # A concurrent force moved the keys: lock the new leaf.
                    chain = current
                    continue
                chain = current
                if self._leaf_ready(chain):
                    return layer_reference(chain.leaf)
                sweep_stale_work_dirs(self.images_dir)
                started = time.time()
                for attempt in range(_MAX_REBASES + 1):
                    try:
                        self._materialize(
                            base,
                            plan,
                            chain,
                            timeout_seconds=timeout_seconds,
                            sandbox_options=sandbox_options,
                            # Bounded: the last attempt builds what it must.
                            allow_rebase=attempt < _MAX_REBASES,
                        )
                        break
                    except _Rebase as rebase:
                        logger.info(
                            "Another build on this node just made image layer "
                            "%s; continuing from what it left instead of "
                            "building it again",
                            rebase.key[:12],
                        )
                logger.info(
                    "Built image %s (%d layers) in %.1fs",
                    chain.leaf[:12],
                    len(plan.steps),
                    time.time() - started,
                )
                return layer_reference(chain.leaf)
        raise SandboxCreationError(
            "Image layer keys kept changing under concurrent force_build "
            "rebuilds; try again."
        )

    # -- keys --------------------------------------------------------------

    def _base_key(self, base: str) -> str:
        tar_stat = None
        if os.path.isfile(base):
            st = os.stat(base)
            tar_stat = (st.st_size, st.st_mtime_ns)
        return base_key(base, get_platform_arch(), tar_stat)

    def _resolve_chain(
        self, base: str, plan: ImageBuildPlan, held: Optional[str] = None
    ) -> _Chain:
        """Compute every layer's key, claiming the epochs of forced layers.

        A forced layer that has not been rebuilt for its token yet claims a
        new epoch *before* its descendants' keys are computed: they are built
        on the rebuilt layer, so they must not resolve to layers built on the
        old one -- here or in any other image.

        Args:
            base: The base image reference.
            plan: The steps.
            held: A key whose lock the caller already holds (the leaf's), so
                is not taken again: a second flock from this process would
                wait on the first.

        Returns:
            The chain: every key, base first, and the forced layers still to
            be rebuilt.
        """
        keys = [self._base_key(base)]
        forced: Set[int] = set()
        secret = self._env_key() if _has_build_env(plan) else None
        for index, step in enumerate(plan.steps, start=1):
            parent = keys[-1]
            key = step_key(parent, read_epoch(self.images_dir, parent), step, secret)
            keys.append(key)
            if step.force_token is not None and not self._force_used(key, step):
                forced.add(index)
                with contextlib.nullcontext() if key == held else self._lock(key):
                    if not self._force_used(key, step):
                        claim_epoch(self.images_dir, key, step.force_token)
        return _Chain(keys, forced)

    def _env_key(self) -> bytes:
        if self._build_env_key is None:
            self._build_env_key = load_build_env_key(self.images_dir)
        return self._build_env_key

    def _force_used(self, key: str, step: BuildStep) -> bool:
        meta = read_layer_json(self._entry_dir(key))
        return meta is not None and meta.get("force_token") == step.force_token

    def _still_forced(self, key: str, step: BuildStep) -> bool:
        """Whether ``step`` is forced and its layer not yet rebuilt for the token.

        Read from the cache each time rather than from the chain: another
        build forcing with the same token -- another image on the same forced
        base, or any image under ``MODAL_FORCE_BUILD`` -- may have rebuilt
        the layer since the chain was resolved, and its rebuild serves this
        build too.
        """
        return step.force_token is not None and not self._force_used(key, step)

    # -- cache entries -----------------------------------------------------

    def _entry_dir(self, key_or_ref: str) -> str:
        ref = key_or_ref if "@" in key_or_ref else layer_reference(key_or_ref)
        return os.path.join(self.images_dir, sanitize_image_name(ref))

    @contextlib.contextmanager
    def _lock(self, key_or_ref: str, waiting: Optional[str] = None):
        """The entry's build lock, held while producing it.

        Only producers take it -- builds, and pulls of a registry image -- so
        one entry is produced once. Sandboxes pinning the published entry,
        and eviction, take the entry's short lock instead, which a build
        holds only for the swap in :meth:`_publish`: a ready image never
        waits for a build of itself.

        ``key_or_ref`` is a layer key or an image reference. With
        ``waiting`` -- what a build holding the lock is building, "image" or
        "layer" -- a busy lock's wait is logged, so that a sandbox create
        waiting on another build does not look stuck.
        """
        ref = key_or_ref if "@" in key_or_ref else layer_reference(key_or_ref)
        path = entry_build_lock_path(self.images_dir, sanitize_image_name(ref))
        os.makedirs(os.path.dirname(path), mode=0o777, exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            try:
                fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                if waiting is not None:
                    logger.info(
                        "Waiting for a build of %s %s already running on this node",
                        waiting,
                        ref.rsplit("@", 1)[-1][:12],
                    )
                # Polled rather than blocking, so that a cancelled build stops
                # waiting.
                delay = 0.05
                while True:
                    self._check_cancelled()
                    time.sleep(delay)
                    try:
                        fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
                        break
                    except BlockingIOError:
                        delay = min(delay * 2, 0.5)
            try:
                yield
            finally:
                fcntl.flock(f, fcntl.LOCK_UN)

    def _complete(self, entry_dir: str) -> bool:
        try:
            with open(os.path.join(entry_dir, _MARKER), "r", encoding="utf-8") as f:
                return f.read() == expected_extract_marker()
        except OSError:
            return False

    def _cached_layer(self, key: str) -> Optional[Dict]:
        """Layer ``key``'s ``layer.json``, if its diff is cached; else None."""
        entry = self._entry_dir(key)
        if not (
            self._complete(entry) and os.path.isfile(os.path.join(entry, LAYER_TAR))
        ):
            return None
        return read_layer_json(entry)

    def _has_layer(self, key: str) -> bool:
        return self._cached_layer(key) is not None

    def _has_rootfs(self, key: str) -> bool:
        entry = self._entry_dir(key)
        return self._has_layer(key) and os.path.isfile(
            os.path.join(entry, ROOTFS_IMAGE)
        )

    def _leaf_ready(self, chain: _Chain) -> bool:
        return not chain.forced and self._has_rootfs(chain.leaf)

    def _touch(self, key: str) -> None:
        try:
            os.utime(os.path.join(self._entry_dir(key), _MARKER))
        except OSError:
            pass

    # -- building ----------------------------------------------------------

    def _replay_reach(
        self, chain: _Chain, plan: ImageBuildPlan, index: int, build_id: str
    ) -> int:
        """How far cached diffs replay from build ``build_id`` of layer ``index``.

        Each next diff must be cached, not still forced, and made on exactly
        the build before it.

        Args:
            chain: The plan's keys.
            plan: The steps.
            index: Where the replay starts; 0 is the base.
            build_id: The build there.

        Returns:
            The last index reached by replaying alone: ``index`` if none.
        """
        reach = index
        for i in range(index + 1, len(plan.steps) + 1):
            key = chain.keys[i]
            if self._still_forced(key, plan.steps[i - 1]):
                break
            meta = self._cached_layer(key)
            if meta is None or meta.get("parent_build_id") != build_id:
                break
            build_id = meta["build_id"]
            reach = i
        return reach

    def _start_candidates(
        self, base: str, plan: ImageBuildPlan, chain: _Chain
    ) -> List[Tuple[int, int]]:
        """The cached images a build could start from, best first.

        Candidates are each layer of the plan cached with its own image,
        below the first layer still forced and below the leaf, and the base
        if it is cached. Best is the one whose cached diffs replay furthest,
        which leaves the fewest steps to run; among those, the deepest, which
        leaves the least to replay. The deepest image alone is not always
        best: the diffs above it may have been made on another build of it.

        Args:
            base: The base image reference.
            plan: The steps.
            chain: Their keys.

        Returns:
            ``(replay reach, index)`` pairs; index 0 is the base.
        """
        n = len(plan.steps)
        limit = n
        for i, step in enumerate(plan.steps, start=1):
            if self._still_forced(chain.keys[i], step):
                limit = min(limit, i)
                break
        candidates = []
        for index in range(1, limit):
            key = chain.keys[index]
            meta = self._cached_layer(key)
            if meta is None or not os.path.isfile(
                os.path.join(self._entry_dir(key), ROOTFS_IMAGE)
            ):
                continue
            reach = self._replay_reach(chain, plan, index, meta["build_id"])
            candidates.append((reach, index))
        base_dir = self._image_manager.get_image_dir(base)
        if self._complete(base_dir):
            build_id = base_build_id(base_dir, chain.keys[0])
            candidates.append((self._replay_reach(chain, plan, 0, build_id), 0))
        return sorted(candidates, reverse=True)

    def _covered(
        self, base: str, plan: ImageBuildPlan, chain: _Chain, index: int
    ) -> bool:
        """Whether the cache now supplies layer ``index`` without building it."""
        return any(
            reach >= index for reach, _ in self._start_candidates(base, plan, chain)
        )

    def _start_point(
        self,
        base: str,
        plan: ImageBuildPlan,
        chain: _Chain,
        timeout: float,
        token: str,
    ) -> Tuple[int, str, str, str]:
        """The image to build from: the one that leaves the fewest steps to run.

        See :meth:`_start_candidates`. The image returned is pinned for
        ``token`` until the caller releases it, so the build reads the image
        it started from to the end. Without the pin, eviction could remove
        the base between here and the first build sandbox, which would then
        pull the tag again and might run the step on a newer image than the
        one being built on.

        Args:
            base: The base image reference.
            plan: The steps.
            chain: Their keys.
            timeout: Network timeout for pulling the base.
            token: The build's in-use token.

        Returns:
            ``(index, reference, entry directory, build id)``; index 0 is the
            base.
        """
        for _, index in self._start_candidates(base, plan, chain):
            if index == 0:
                break
            ref = layer_reference(chain.keys[index])
            # Pinned first and checked after: once pinned, eviction can no
            # longer take its image away, so what the check sees holds.
            try:
                entry = self._image_manager.pull_image(ref, instance_id=token)
            except BuiltImageEvictedError:
                continue
            meta = read_layer_json(entry)
            if meta is None or not os.path.isfile(os.path.join(entry, ROOTFS_IMAGE)):
                self._image_manager.release_image(ref, token)
                continue
            return index, ref, entry, meta["build_id"]
        base_dir = self._image_manager.pull_image(
            base, timeout_seconds=timeout, instance_id=token
        )
        return 0, base, base_dir, base_build_id(base_dir, chain.keys[0])

    def _materialize(
        self,
        base: str,
        plan: ImageBuildPlan,
        chain: _Chain,
        *,
        timeout_seconds: float,
        sandbox_options: Dict,
        allow_rebase: bool = True,
    ) -> None:
        """Build the leaf of ``chain``; the caller holds its lock.

        Args:
            base: The base image reference.
            plan: The steps.
            chain: Their keys.
            timeout_seconds: Network and sandbox-start timeout.
            sandbox_options: Extra ``SandboxConfig`` fields for build sandboxes.
            allow_rebase: Start over, rather than build a layer, when the
                cache has meanwhile come to supply it.

        Raises:
            _Rebase: ``allow_rebase`` is set, and another build has just made
                a layer this one was about to build, in a form this build can
                start over from (see :class:`_Rebase`).
        """
        n = len(plan.steps)
        work_ref = work_reference(chain.leaf)
        work_dir = os.path.join(self.images_dir, sanitize_image_name(work_ref))
        work_image = os.path.join(work_dir, ROOTFS_IMAGE)
        # Pins the work copy, and the image it starts from, for this build.
        token = build_token()
        start_ref: Optional[str] = None
        # Build sandboxes still being deleted, with the image each one runs on.
        teardowns: List[Tuple[threading.Thread, str]] = []
        os.makedirs(work_dir)
        owner = None
        try:
            # Held until the work copy is gone: if this process dies first,
            # a sweep removes the work copy and releases the token's pins.
            owner = own_work_dir(work_dir, token)
            _mark_image_in_use(work_dir, token)
            evict_to_cache_limits(self.images_dir, keep=os.path.basename(work_dir))
            for attempt in range(2):
                start, start_ref, start_dir, parent_build_id = self._start_point(
                    base, plan, chain, timeout_seconds, token
                )
                try:
                    # Copied while the first build step, if one has to run,
                    # works on the start image itself: read-only to the
                    # sandbox, whose writes go to its own overlay.
                    copier = _BackgroundCopy(
                        os.path.join(start_dir, ROOTFS_IMAGE), work_image
                    )
                    break
                except FileNotFoundError:
                    # Republished between pinning and opening it; look again.
                    self._image_manager.release_image(start_ref, token)
                    start_ref = None
                    if attempt:
                        raise
            config_src = os.path.join(start_dir, _IMAGE_CONFIG)
            if os.path.isfile(config_src):
                shutil.copyfile(config_src, os.path.join(work_dir, _IMAGE_CONFIG))

            def work_ready() -> str:
                """Wait for the copy; the work image is usable from here on."""
                copier.wait()
                marker = os.path.join(work_dir, _MARKER)
                if not os.path.exists(marker):
                    with open(marker, "w", encoding="utf-8") as f:
                        f.write(expected_extract_marker())
                return work_ref

            def before_apply() -> None:
                """Wait out the deletion of build sandboxes on the work copy.

                Applying a layer rewrites the work copy in place, and such a
                sandbox maps it until runsc has torn it down. One that ran on
                the start image maps nothing the build writes, so its
                deletion carries on behind the rest of the build.
                """
                for thread, image in teardowns:
                    if image == work_ref:
                        thread.join()

            run_image = start_ref
            for index in range(start + 1, n + 1):
                self._check_cancelled()
                step = plan.steps[index - 1]
                key = chain.keys[index]
                is_leaf = index == n
                reused = self._reusable_layer(key, step, parent_build_id, work_dir)
                if reused is None:
                    # The leaf's lock is held already; taking it again from
                    # this process would wait on itself.
                    with (
                        contextlib.nullcontext()
                        if is_leaf
                        else self._lock(key, waiting="layer")
                    ):
                        # Again under the lock: a build that held it may have
                        # produced this layer meanwhile.
                        reused = self._reusable_layer(
                            key, step, parent_build_id, work_dir
                        )
                        if (
                            reused is None
                            and allow_rebase
                            and self._covered(base, plan, chain, index)
                        ):
                            # Made meanwhile, but not on this work copy's
                            # lineage. Building it again would also replace
                            # what that build published.
                            raise _Rebase(key)
                        if reused is None:
                            layer, info, build_id = self._produce(
                                step,
                                key,
                                parent_build_id,
                                run_image=run_image,
                                work_dir=work_dir,
                                work_ready=work_ready,
                                teardowns=teardowns,
                                timeout_seconds=timeout_seconds,
                                sandbox_options=sandbox_options,
                            )
                            work_ready()
                            before_apply()
                            # Applied before publishing: once published, the
                            # entry is evictable and the diff may vanish.
                            apply_layer_to_erofs(work_image, layer)
                            _apply_image_env(step, work_dir)
                            meta = self._layer_meta(
                                chain,
                                index,
                                base,
                                step,
                                parent_build_id,
                                build_id,
                                info,
                            )
                            if is_leaf:
                                self._publish_leaf(key, work_dir, layer, meta)
                            else:
                                self._publish(key, {LAYER_TAR: layer}, meta)
                            if index in chain.forced:
                                logger.info("Rebuilt forced image layer %s", key[:12])
                            parent_build_id = build_id
                if reused is not None:
                    link, meta = reused
                    work_ready()
                    before_apply()
                    apply_layer_to_erofs(work_image, link)
                    _apply_image_env(step, work_dir)
                    os.remove(link)
                    parent_build_id = meta["build_id"]
                    self._touch(key)
                    if is_leaf:
                        self._publish_leaf(key, work_dir, None, meta)
                run_image = work_ref
        finally:
            # Not waited for: the image is published, and only a build
            # sandbox that ran on the start image can still be going away
            # (those on the work copy were waited for before each apply).
            # It holds its own pin on that image until it is gone.
            if start_ref is not None:
                self._image_manager.release_image(start_ref, token)
            shutil.rmtree(work_dir, ignore_errors=True)
            # Pulls of the work copy made its lock file; nothing else ever
            # uses that name again.
            with contextlib.suppress(OSError):
                os.remove(f"{work_dir}.lock")
            # Last: until the work copy is gone, a sweep must see it owned.
            if owner is not None:
                owner.close()

    def _reusable_layer(
        self, key: str, step: BuildStep, parent_build_id: str, work_dir: str
    ) -> Optional[Tuple[str, Dict]]:
        """A private hard link to layer ``key``'s diff, if it may be replayed here.

        It may if it is cached, not still forced (see :meth:`_still_forced`),
        and was built on exactly the parent build it is about to be applied
        to. The link keeps the diff readable even if eviction removes the
        entry meanwhile.

        Args:
            key: The layer's key.
            step: Its step.
            parent_build_id: The build it would be applied on.
            work_dir: Where to put the link.

        Returns:
            ``(link, layer.json)``, or None to build the layer instead.
        """
        if self._still_forced(key, step):
            return None
        meta = self._cached_layer(key)
        if meta is None or meta.get("parent_build_id") != parent_build_id:
            return None
        entry = self._entry_dir(key)
        link = os.path.join(work_dir, f"reuse-{key[:16]}.tar")
        try:
            os.link(os.path.join(entry, LAYER_TAR), link)
        except FileNotFoundError:
            return None
        except OSError:
            try:
                shutil.copyfile(os.path.join(entry, LAYER_TAR), link)
            except FileNotFoundError:
                return None
        return link, meta

    def _produce(
        self,
        step: BuildStep,
        key: str,
        parent_build_id: str,
        *,
        run_image: str,
        work_dir: str,
        work_ready: Callable[[], str],
        teardowns: List[Tuple[threading.Thread, str]],
        timeout_seconds: float,
        sandbox_options: Dict,
    ) -> Tuple[str, LayerInfo, str]:
        """Build one layer's diff into ``work_dir``.

        Args:
            step: The step.
            key: Its layer's key.
            parent_build_id: The build of the layer below it.
            run_image: The image a RUN step's build sandbox starts from.
            work_dir: The build's private directory.
            work_ready: Waits until the work copy of the image is usable.
            teardowns: Where a RUN step leaves its build sandbox's deletion,
                still running, with the image that sandbox ran on.
            timeout_seconds: Network and sandbox-start timeout.
            sandbox_options: Extra ``SandboxConfig`` fields for build sandboxes.

        Returns:
            ``(layer path, its sha256 and size, build id)``.
        """
        layer = os.path.join(work_dir, f"layer-{key[:16]}.tar")
        logger.info("[image build %s] %s", key[:12], step.summary())
        if isinstance(step, RunStep):
            info = self._run_step(
                step,
                key,
                run_image,
                work_dir,
                layer,
                work_ready,
                teardowns,
                timeout_seconds=timeout_seconds,
                sandbox_options=sandbox_options,
            )
            return layer, info, f"run:{uuid.uuid4().hex}"
        work_ready()
        work_image = os.path.join(work_dir, ROOTFS_IMAGE)
        archive = os.path.join(work_dir, f"copy-{key[:16]}.tar")
        if isinstance(step, CopyStep):
            _write_archive(step, archive)
            build_id = _hash(parent_build_id, step.digest)
        elif isinstance(step, CopyFromImageStep):
            src_dir = self._image_manager.pull_image(
                step.image, timeout_seconds=timeout_seconds
            )
            archive_path_from_image(
                os.path.join(src_dir, ROOTFS_IMAGE), step.src, step.dest, archive
            )
            build_id = _hash(
                parent_build_id, base_build_id(src_dir, step.image), step.src, step.dest
            )
        else:  # pragma: no cover - ImageBuildPlan validates step types
            raise TypeError(f"Not a build step: {step!r}")
        try:
            with ErofsImage(work_image) as image:
                info = prepare_copy_layer(image, archive, layer)
        except ErofsError as err:
            raise SandboxImageBuildError(
                f"Cannot read the image being built to place copied files: {err}", key
            ) from err
        finally:
            os.remove(archive)
        return layer, info, build_id

    def _run_step(
        self,
        step: RunStep,
        key: str,
        image: str,
        work_dir: str,
        layer: str,
        work_ready: Callable[[], str],
        teardowns: List[Tuple[threading.Thread, str]],
        *,
        timeout_seconds: float,
        sandbox_options: Dict,
    ) -> LayerInfo:
        """Run ``step`` in a build sandbox on ``image`` and write its diff to ``layer``.

        Args:
            step: The step.
            key: Its layer's key.
            image: The image the build sandbox starts from.
            work_dir: The build's private directory.
            layer: Where to write the layer.
            work_ready: Waits until the work copy of the image is usable.
            teardowns: Where the build sandbox's deletion is left running,
                with the image the sandbox ran on.
            timeout_seconds: How long the build sandbox may take to start.
            sandbox_options: Extra ``SandboxConfig`` fields.

        Returns:
            The layer's sha256 and size.
        """
        config = SandboxConfig(
            image=image,
            env={**dict(step.env), **dict(step.build_env)},
            workdir=step.workdir,
            readonly=False,
            capabilities=list(DOCKER_DEFAULT_CAPABILITIES),
            timeout_seconds=timeout_seconds,
            **sandbox_options,
        )
        sandbox_id = self._backend.create_sandbox(config)
        deleter = None
        raw = os.path.join(work_dir, f"upper-{key[:16]}.tar")
        # In the build's private directory, which no sandbox mounts: runsc
        # writes it from the host.
        pid_file = os.path.join(work_dir, f"pid-{key[:16]}")
        try:
            hint = (
                f"\n(Build steps run without network access: {_BUILD_NETWORK_ENV}"
                "=none on this node.)"
                if config.network == "none"
                else ""
            )
            for command in step.commands:
                self._check_cancelled()
                self._exec(sandbox_id, step, command, key, pid_file, hint)
            self._check_cancelled()
            self._backend.export_rootfs_upper(sandbox_id, raw)
            # Deleting a sandbox that wrote a lot takes a while (runsc tears
            # its memory down), and nothing from here on needs it gone: its
            # changes are in the export. The build waits for it only before
            # rewriting an image the sandbox maps, and may return before it
            # is done. Not a daemon, so a process that exits right after a
            # build still deletes the sandbox rather than leaving it running.
            deleter = threading.Thread(
                target=self._delete_quietly,
                args=(sandbox_id,),
                name=f"delete-build-sandbox-{sandbox_id}",
            )
            deleter.start()
            teardowns.append((deleter, image))
            work_ready()
            with ErofsImage(os.path.join(work_dir, ROOTFS_IMAGE)) as erofs:
                root = erofs.stat(erofs.root_nid)
            with open(raw, "rb") as src:
                return normalize_overlay_diff(src, layer, root)
        finally:
            if deleter is None:
                self._delete_quietly(sandbox_id)
            with contextlib.suppress(OSError):
                os.remove(raw)

    def _exec(
        self,
        sandbox_id: str,
        step: RunStep,
        command: str,
        key: str,
        pid_file: str,
        hint: str = "",
    ) -> None:
        """Run one of ``step``'s commands, logging its output as it comes.

        Args:
            sandbox_id: The build sandbox.
            step: The step the command belongs to: its shell.
            command: The command.
            key: The layer's key, for the log prefix and errors.
            pid_file: Where runsc records the command's pid in the sandbox.
            hint: Ends the message of a failure, after the output.

        Raises:
            SandboxImageBuildError: The command failed or timed out, and the
                message carries the end of its output; or the build was
                cancelled.
        """
        argv = self._backend.exec_argv(
            sandbox_id, list(step.shell) + [command], pid_file=pid_file
        )
        timeout = _step_timeout()
        proc = subprocess.Popen(
            argv,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )
        output = _OutputTail(f"[image build {key[:12]}]")
        reader = threading.Thread(target=output.pump, args=(proc.stdout,), daemon=True)
        reader.start()
        timed_out = threading.Event()

        def stop() -> None:
            self._kill_in_sandbox(sandbox_id, pid_file)
            proc.kill()

        def time_out() -> None:
            timed_out.set()
            stop()

        # A timer, not a polling watchdog: a command's end is seen at once
        # rather than on the next poll. A cancel stops it the same way.
        timer = threading.Timer(timeout, time_out)
        timer.daemon = True
        timer.start()
        run = self._runs.get(threading.get_ident())
        if run is not None:
            run.running(stop)
        try:
            returncode = proc.wait()
        finally:
            timer.cancel()
            if run is not None:
                run.running(None)
            with contextlib.suppress(OSError):
                os.remove(pid_file)
        # The runsc exec client ends with the command, but anything the
        # command left running in the background holds its output pipe open
        # until the sandbox goes: take what came rather than wait for that.
        reader.join(_OUTPUT_DRAIN_SECONDS)
        text = output.text()
        if run is not None:
            run.check()
        if timed_out.is_set():
            raise SandboxImageBuildError(
                f"Image build step timed out after {timeout:.0f}s "
                f"({_BUILD_TIMEOUT_ENV}): {command}\n{text}{hint}",
                key,
            )
        if returncode != 0:
            raise SandboxImageBuildError(
                f"Image build step failed with exit code {returncode}: {command}\n"
                f"{text}{hint}",
                key,
            )

    def _kill_in_sandbox(self, sandbox_id: str, pid_file: str) -> None:
        """SIGKILL a timed-out command's process group inside its sandbox.

        Killing the ``runsc exec`` client alone leaves the command running --
        SIGKILL cannot be forwarded into the sandbox -- and holding its output
        pipe open. If runsc has not recorded the pid yet, deleting the sandbox
        ends the command instead.
        """
        pid = _read_pid(pid_file)
        if pid is None:
            return
        try:
            subprocess.run(
                self._backend.kill_process_group_argv(sandbox_id, pid),
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                timeout=10,
            )
        except Exception:  # noqa: BLE001
            logger.debug(
                "Could not kill pid %s in build sandbox %s",
                pid,
                sandbox_id,
                exc_info=True,
            )

    def _delete_quietly(self, sandbox_id: str) -> None:
        try:
            self._backend.delete_sandbox(sandbox_id)
        except Exception:  # noqa: BLE001
            logger.warning(
                "Failed to delete build sandbox %s", sandbox_id, exc_info=True
            )

    def _layer_meta(
        self,
        chain: _Chain,
        index: int,
        base: str,
        step: BuildStep,
        parent_build_id: str,
        build_id: str,
        info: LayerInfo,
    ) -> Dict:
        return {
            "builder_version": _builder_version(),
            "key": chain.keys[index],
            "depth": index,
            "base": {"reference": base, "key": chain.keys[0]},
            "parent_key": chain.keys[index - 1] if index > 1 else None,
            "parent_build_id": parent_build_id,
            "build_id": build_id,
            "step": step.record(),
            "force_token": step.force_token,
            "layer": {"sha256": info.sha256, "size": info.size},
            "created": _now_iso(),
        }

    def _publish(
        self,
        key: str,
        files: Dict[str, str],
        meta: Dict,
        extra: Optional[Dict[str, str]] = None,
    ) -> None:
        """Atomically replace entry ``key`` with ``files`` and ``meta``.

        Callers hold the entry's build lock. The swap itself happens under
        the entry's short lock, the one pins and eviction take, held from
        reading the current users to recording them on the new entry: a pin
        can't land in between and be lost, and eviction can't see the new
        entry before its pins are carried over. As with a re-pull, sandboxes
        running on the entry being replaced keep their pins and their open
        image.
        """
        target = self._entry_dir(key)
        tmp = f"{target}.tmp.{uuid.uuid4().hex}"
        os.makedirs(tmp)
        try:
            for name, src in {**files, **(extra or {})}.items():
                os.replace(src, os.path.join(tmp, name))
            write_layer_json(tmp, meta)
            with open(os.path.join(tmp, _MARKER), "w", encoding="utf-8") as f:
                f.write(expected_extract_marker())
            with hold_lock(entry_lock_path(self.images_dir, os.path.basename(target))):
                try:
                    users = os.listdir(os.path.join(target, _USERS_SUBDIR))
                except OSError:
                    users = []
                if os.path.exists(target):
                    shutil.rmtree(target, ignore_errors=True)
                os.replace(tmp, target)
                for user in users:
                    _mark_image_in_use(target, user)
        except BaseException:
            shutil.rmtree(tmp, ignore_errors=True)
            raise

    def _publish_leaf(
        self,
        key: str,
        work_dir: str,
        layer: Optional[str],
        meta: Dict,
    ) -> None:
        """Move the finished image into the leaf entry (whose lock the caller holds).

        The image is checked first: every sandbox on the node that uses it
        would mount a bad one, and nothing would rebuild it while it stays
        cached.
        """
        if os.environ.get(_VERIFY_ENV, "1") != "0":
            verify_erofs_image(os.path.join(work_dir, ROOTFS_IMAGE), key)
        evict_to_cache_limits(
            self.images_dir, keep=sanitize_image_name(layer_reference(key))
        )
        extra = {ROOTFS_IMAGE: os.path.join(work_dir, ROOTFS_IMAGE)}
        config = os.path.join(work_dir, _IMAGE_CONFIG)
        if os.path.isfile(config):
            extra[_IMAGE_CONFIG] = config
        if layer is None:
            # The diff was cached; keep it with the image.
            entry = self._entry_dir(key)
            layer = os.path.join(work_dir, "leaf-layer.tar")
            try:
                os.link(os.path.join(entry, LAYER_TAR), layer)
            except OSError:
                shutil.copyfile(os.path.join(entry, LAYER_TAR), layer)
        self._publish(key, {LAYER_TAR: layer}, meta, extra=extra)


class _OutputTail:
    """Logs a command's output line by line and keeps its last bytes.

    Args:
        prefix: Starts every logged line.
        limit: How many bytes of the output's end to keep.
    """

    def __init__(self, prefix: str, limit: int = _OUTPUT_TAIL_BYTES):
        self._prefix = prefix
        self._limit = limit
        self._lines: Deque[bytes] = deque()
        self._size = 0
        self._lock = threading.Lock()

    def pump(self, stream: BinaryIO) -> None:
        """Read ``stream`` to its end; run in a thread of its own."""
        with stream:
            # Bounded reads: a command that prints without newlines must not
            # make one unbounded line.
            for line in iter(lambda: stream.readline(_OUTPUT_TAIL_BYTES), b""):
                logger.info(
                    "%s %s",
                    self._prefix,
                    line.decode("utf-8", errors="replace").rstrip("\n"),
                )
                with self._lock:
                    self._lines.append(line)
                    self._size += len(line)
                    while self._size > self._limit and len(self._lines) > 1:
                        self._size -= len(self._lines.popleft())

    def text(self) -> str:
        with self._lock:
            return b"".join(self._lines).decode("utf-8", errors="replace")


def _read_pid(path: str) -> Optional[int]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return int(f.read().strip())
    except (OSError, ValueError):
        return None


class _BackgroundCopy:
    """Copy a file in a thread, in the kernel where possible."""

    def __init__(self, src: str, dst: str):
        self._error: Optional[BaseException] = None
        # Opened now, so an eviction that removes the source meanwhile cannot
        # take it away from the copy.
        self._src = open(src, "rb")
        self._dst = dst
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def _run(self) -> None:
        try:
            with self._src, open(self._dst, "wb") as out:
                size = os.fstat(self._src.fileno()).st_size
                offset = 0
                try:
                    while offset < size:
                        copied = os.copy_file_range(
                            self._src.fileno(), out.fileno(), size - offset
                        )
                        if copied == 0:
                            break
                        offset += copied
                except OSError:
                    self._src.seek(offset)
                    out.seek(offset)
                    shutil.copyfileobj(self._src, out, 1 << 20)
        except BaseException as err:  # noqa: BLE001
            self._error = err

    def wait(self) -> None:
        self._thread.join()
        if self._error is not None:
            error, self._error = self._error, None
            raise SandboxCreationError(
                f"Failed to copy the image to build on: {error}"
            ) from error


_ENV_REF = re.compile(r"\$(?:\{(\w+)\}|(\w+))")


def _expand(value: str, env: Dict[str, str]) -> str:
    """Docker's ``ENV`` expansion: ``$VAR`` and ``${VAR}``, unset ones empty."""
    return _ENV_REF.sub(lambda m: env.get(m.group(1) or m.group(2), ""), value)


def _apply_image_env(step: BuildStep, work_dir: str) -> None:
    """Apply a RUN step's ``image_env`` to the work copy's image config."""
    if not isinstance(step, RunStep) or not step.image_env:
        return
    path = os.path.join(work_dir, _IMAGE_CONFIG)
    try:
        with open(path, "r", encoding="utf-8") as f:
            config = json.load(f)
    except (OSError, ValueError):
        config = {}
    inner = config.setdefault("config", {})
    entries = list(inner.get("Env") or [])
    current = dict(entry.split("=", 1) for entry in entries if "=" in entry)
    for name, value in step.image_env:
        current[name] = _expand(value, current)
    order = [entry.split("=", 1)[0] for entry in entries]
    order += [name for name, _ in step.image_env if name not in order]
    inner["Env"] = [
        f"{name}={current[name]}" for name in dict.fromkeys(order) if name in current
    ]
    with open(path, "w", encoding="utf-8") as f:
        json.dump(config, f)


def _write_archive(step: CopyStep, path: str) -> None:
    """Fetch a COPY step's archive to ``path``."""
    archive = step.archive
    if archive is None:
        raise SandboxImageBuildError(
            f"COPY step {step.summary()} carries no files to copy."
        )
    import ray

    if isinstance(archive, ray.ObjectRef):
        # Builds run in a worker thread of the sandbox's async actor, where
        # ray.get would block nothing but still warns that it might; waiting
        # on the ref's future is the same wait without the warning. The
        # client puts a uint8 array, so this is a view of the object store,
        # not a copy.
        archive = archive.future().result()
    with open(path, "wb") as f:
        f.write(memoryview(archive))


def _has_build_env(plan: ImageBuildPlan) -> bool:
    return any(isinstance(step, RunStep) and step.build_env for step in plan.steps)


def _builder_version() -> str:
    from ray.experimental.sandbox.image_build import BUILDER_VERSION

    return BUILDER_VERSION
