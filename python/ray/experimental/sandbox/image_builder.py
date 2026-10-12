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

Locking: a build holds its leaf's lock throughout, and at most one ancestor
layer's lock at a time, only while producing that layer. A builder therefore
only ever waits, holding nothing but its own leaf, for a strictly shallower
entry, so no cycle can form.
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
from typing import Callable, Dict, List, Optional, Set, Tuple

from ray.experimental.sandbox._internal.erofs_reader import ErofsError, ErofsImage
from ray.experimental.sandbox._internal.image_layers import (
    LAYER_TAR,
    apply_layer_to_erofs,
    archive_path_from_image,
    base_build_id,
    claim_epoch,
    layer_reference,
    normalize_overlay_diff,
    prepare_copy_layer,
    read_epoch,
    read_layer_json,
    require_incremental_mkfs_erofs,
    sweep_stale_work_dirs,
    work_reference,
    write_builder_file,
    write_layer_json,
)
from ray.experimental.sandbox._internal.image_utils import (
    _USERS_SUBDIR,
    ROOTFS_IMAGE,
    _mark_image_in_use,
    evict_least_recently_used_images,
    expected_extract_marker,
    get_platform_arch,
    image_cache_max_bytes,
    sanitize_image_name,
)
from ray.experimental.sandbox.backend.base import BaseSandboxBackend
from ray.experimental.sandbox.config import DOCKER_DEFAULT_CAPABILITIES, SandboxConfig
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
# How much of a failed step's output its error message carries.
_OUTPUT_TAIL_BYTES = 64 * 1024
_MARKER = ".extracted"
_IMAGE_CONFIG = ".image_config.json"


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


def _now_iso() -> str:
    return datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _hash(*parts: str) -> str:
    return "sha256:" + hashlib.sha256("\0".join(parts).encode("utf-8")).hexdigest()


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

    @property
    def images_dir(self) -> str:
        return self._image_manager.images_dir

    # -- public ------------------------------------------------------------

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
            SandboxImageBuildError: A step failed.
            SandboxCreationError: The node cannot build (no runsc, an old
                mkfs.erofs) or the base cannot be pulled.
        """
        if base_force_token is not None:
            key = self._base_key(base)
            with self._lock(key):
                claim_epoch(self.images_dir, key, base_force_token)
        chain = self._resolve_chain(base, plan)
        if self._leaf_ready(chain):
            self._touch(chain.leaf)
            return layer_reference(chain.leaf)
        require_incremental_mkfs_erofs()
        for _ in range(5):
            with self._lock(chain.leaf):
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
                self._materialize(
                    base,
                    plan,
                    chain,
                    timeout_seconds=timeout_seconds,
                    sandbox_options={
                        "rootless": rootless,
                        "_ignore_cgroups": ignore_cgroups,
                    },
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
        for index, step in enumerate(plan.steps, start=1):
            parent = keys[-1]
            key = step_key(parent, read_epoch(self.images_dir, parent), step)
            keys.append(key)
            if step.force_token is not None and not self._force_used(key, step):
                forced.add(index)
                with contextlib.nullcontext() if key == held else self._lock(key):
                    if not self._force_used(key, step):
                        claim_epoch(self.images_dir, key, step.force_token)
        return _Chain(keys, forced)

    def _force_used(self, key: str, step: BuildStep) -> bool:
        meta = read_layer_json(self._entry_dir(key))
        return meta is not None and meta.get("force_token") == step.force_token

    # -- cache entries -----------------------------------------------------

    def _entry_dir(self, key_or_ref: str) -> str:
        ref = key_or_ref if "@" in key_or_ref else layer_reference(key_or_ref)
        return os.path.join(self.images_dir, sanitize_image_name(ref))

    @contextlib.contextmanager
    def _lock(self, key_or_ref: str):
        """The entry's flock: the one pulls and eviction take for it too."""
        ref = key_or_ref if "@" in key_or_ref else layer_reference(key_or_ref)
        os.makedirs(self.images_dir, mode=0o777, exist_ok=True)
        path = os.path.join(self.images_dir, f"{sanitize_image_name(ref)}.lock")
        with open(path, "w", encoding="utf-8") as f:
            fcntl.flock(f, fcntl.LOCK_EX)
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

    def _has_layer(self, key: str) -> bool:
        entry = self._entry_dir(key)
        return (
            self._complete(entry)
            and os.path.isfile(os.path.join(entry, LAYER_TAR))
            and read_layer_json(entry) is not None
        )

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

    def _start_point(
        self, base: str, plan: ImageBuildPlan, chain: _Chain, timeout: float
    ) -> Tuple[int, str, str, str]:
        """The deepest image to build from: a cached leaf of a prefix, or the base.

        Args:
            base: The base image reference.
            plan: The steps.
            chain: Their keys.
            timeout: Network timeout for pulling the base.

        Returns:
            ``(index, reference, entry directory, build id)``; index 0 is the
            base.
        """
        first_forced = min(chain.forced) if chain.forced else len(plan.steps) + 1
        for index in range(len(plan.steps) - 1, 0, -1):
            if index >= first_forced:
                continue
            key = chain.keys[index]
            if self._has_rootfs(key):
                meta = read_layer_json(self._entry_dir(key))
                return (
                    index,
                    layer_reference(key),
                    self._entry_dir(key),
                    meta["build_id"],
                )
        base_dir = self._image_manager.pull_image(base, timeout_seconds=timeout)
        return 0, base, base_dir, base_build_id(base_dir, chain.keys[0])

    def _materialize(
        self,
        base: str,
        plan: ImageBuildPlan,
        chain: _Chain,
        *,
        timeout_seconds: float,
        sandbox_options: Dict,
    ) -> None:
        """Build the leaf of ``chain``; the caller holds its lock."""
        n = len(plan.steps)
        max_bytes = image_cache_max_bytes(self.images_dir)
        work_ref = work_reference(chain.leaf)
        work_dir = os.path.join(self.images_dir, sanitize_image_name(work_ref))
        work_image = os.path.join(work_dir, ROOTFS_IMAGE)
        os.makedirs(work_dir)
        try:
            write_builder_file(work_dir)
            _mark_image_in_use(work_dir, f"build-{uuid.uuid4().hex[:12]}")
            if max_bytes > 0:
                evict_least_recently_used_images(
                    self.images_dir, max_bytes, keep=os.path.basename(work_dir)
                )
            for attempt in range(2):
                start, start_ref, start_dir, parent_build_id = self._start_point(
                    base, plan, chain, timeout_seconds
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
                    # Evicted between finding and opening it; look again.
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

            run_image = start_ref
            for index in range(start + 1, n + 1):
                step = plan.steps[index - 1]
                key = chain.keys[index]
                is_leaf = index == n
                reused = self._reusable_layer(
                    key, index, chain, parent_build_id, work_dir
                )
                if reused is None:
                    # The leaf's lock is held already; taking it again from
                    # this process would wait on itself.
                    with contextlib.nullcontext() if is_leaf else self._lock(key):
                        # Again under the lock: a build that held it may have
                        # produced this layer meanwhile.
                        reused = self._reusable_layer(
                            key, index, chain, parent_build_id, work_dir
                        )
                        if reused is None:
                            layer, build_id = self._produce(
                                step,
                                key,
                                parent_build_id,
                                run_image=run_image,
                                work_dir=work_dir,
                                work_ready=work_ready,
                                timeout_seconds=timeout_seconds,
                                sandbox_options=sandbox_options,
                            )
                            work_ready()
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
                                layer,
                            )
                            if is_leaf:
                                self._publish_leaf(
                                    key, work_dir, layer, meta, max_bytes
                                )
                            else:
                                self._publish(key, {LAYER_TAR: layer}, meta)
                            if index in chain.forced:
                                logger.info("Rebuilt forced image layer %s", key[:12])
                            parent_build_id = build_id
                if reused is not None:
                    link, meta = reused
                    work_ready()
                    apply_layer_to_erofs(work_image, link)
                    _apply_image_env(step, work_dir)
                    os.remove(link)
                    parent_build_id = meta["build_id"]
                    self._touch(key)
                    if is_leaf:
                        self._publish_leaf(key, work_dir, None, meta, max_bytes)
                run_image = work_ref
        finally:
            shutil.rmtree(work_dir, ignore_errors=True)
            # Pulls of the work copy made its lock file; nothing else ever
            # uses that name again.
            with contextlib.suppress(OSError):
                os.remove(f"{work_dir}.lock")

    def _reusable_layer(
        self, key: str, index: int, chain: _Chain, parent_build_id: str, work_dir: str
    ) -> Optional[Tuple[str, Dict]]:
        """A private hard link to layer ``key``'s diff, if it may be replayed here.

        It may if it is cached, not forced, and was built on exactly the
        parent build it is about to be applied to. The link keeps the diff
        readable even if eviction removes the entry meanwhile.

        Args:
            key: The layer's key.
            index: Its position in the chain, 1-based.
            chain: The chain being built.
            parent_build_id: The build it would be applied on.
            work_dir: Where to put the link.

        Returns:
            ``(link, layer.json)``, or None to build the layer instead.
        """
        if index in chain.forced or not self._has_layer(key):
            return None
        entry = self._entry_dir(key)
        meta = read_layer_json(entry)
        if meta is None or meta.get("parent_build_id") != parent_build_id:
            return None
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
        timeout_seconds: float,
        sandbox_options: Dict,
    ) -> Tuple[str, str]:
        """Build one layer's diff into ``work_dir``.

        Args:
            step: The step.
            key: Its layer's key.
            parent_build_id: The build of the layer below it.
            run_image: The image a RUN step's build sandbox starts from.
            work_dir: The build's private directory.
            work_ready: Waits until the work copy of the image is usable.
            timeout_seconds: Network and sandbox-start timeout.
            sandbox_options: Extra ``SandboxConfig`` fields for build sandboxes.

        Returns:
            ``(layer path, build id)``.
        """
        layer = os.path.join(work_dir, f"layer-{key[:16]}.tar")
        logger.info("[image build %s] %s", key[:12], step.summary())
        if isinstance(step, RunStep):
            self._run_step(
                step,
                key,
                run_image,
                work_dir,
                layer,
                work_ready,
                timeout_seconds=timeout_seconds,
                sandbox_options=sandbox_options,
            )
            return layer, f"run:{uuid.uuid4().hex}"
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
                prepare_copy_layer(image, archive, layer)
        except ErofsError as err:
            raise SandboxImageBuildError(
                f"Cannot read the image being built to place copied files: {err}", key
            ) from err
        finally:
            os.remove(archive)
        return layer, build_id

    def _run_step(
        self,
        step: RunStep,
        key: str,
        image: str,
        work_dir: str,
        layer: str,
        work_ready,
        *,
        timeout_seconds: float,
        sandbox_options: Dict,
    ) -> None:
        """Run ``step`` in a build sandbox on ``image`` and write its diff to ``layer``."""
        config = SandboxConfig(
            image=image,
            env={**dict(step.env), **dict(step.build_env)},
            workdir=step.workdir,
            readonly=False,
            network="public",
            capabilities=list(DOCKER_DEFAULT_CAPABILITIES),
            timeout_seconds=timeout_seconds,
            **sandbox_options,
        )
        sandbox_id = self._backend.create_sandbox(config)
        deleter = None
        raw = os.path.join(work_dir, f"upper-{key[:16]}.tar")
        try:
            for command in step.commands:
                self._exec(sandbox_id, step, command, key)
            self._backend.export_rootfs_upper(sandbox_id, raw)
            # Deleting a sandbox that wrote a lot takes a while (runsc tears
            # its memory down); it overlaps with normalizing the diff, which
            # only reads the export.
            deleter = threading.Thread(
                target=self._delete_quietly, args=(sandbox_id,), daemon=True
            )
            deleter.start()
            work_ready()
            with ErofsImage(os.path.join(work_dir, ROOTFS_IMAGE)) as image:
                root = image.stat(image.root_nid)
            with open(raw, "rb") as src:
                normalize_overlay_diff(src, layer, root)
        finally:
            if deleter is None:
                self._delete_quietly(sandbox_id)
            else:
                # The next apply rewrites the image this sandbox may map.
                deleter.join()
            with contextlib.suppress(OSError):
                os.remove(raw)

    def _exec(self, sandbox_id: str, step: RunStep, command: str, key: str) -> None:
        argv = self._backend.exec_argv(sandbox_id, list(step.shell) + [command])
        tail: deque = deque()
        tail_size = 0
        timeout = _step_timeout()
        deadline = time.monotonic() + timeout
        proc = subprocess.Popen(
            argv,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )
        timed_out = threading.Event()

        def _watchdog():
            while proc.poll() is None:
                if time.monotonic() > deadline:
                    timed_out.set()
                    # The runsc exec client dies with SIGKILL, but the command
                    # inside the sandbox would not; deleting the sandbox (in
                    # the caller's cleanup) ends it.
                    proc.kill()
                    return
                time.sleep(0.5)

        watchdog = threading.Thread(target=_watchdog, daemon=True)
        watchdog.start()
        prefix = f"[image build {key[:12]}]"
        for line in proc.stdout:
            text = line.decode("utf-8", errors="replace").rstrip("\n")
            logger.info("%s %s", prefix, text)
            tail.append(line)
            tail_size += len(line)
            while tail_size > _OUTPUT_TAIL_BYTES and len(tail) > 1:
                tail_size -= len(tail.popleft())
        returncode = proc.wait()
        watchdog.join()
        output = b"".join(tail).decode("utf-8", errors="replace")
        if timed_out.is_set():
            raise SandboxImageBuildError(
                f"Image build step timed out after {timeout:.0f}s "
                f"({_BUILD_TIMEOUT_ENV}): {command}\n{output}",
                key,
            )
        if returncode != 0:
            raise SandboxImageBuildError(
                f"Image build step failed with exit code {returncode}: {command}\n"
                f"{output}",
                key,
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
        layer: str,
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
            "layer": {"size": os.path.getsize(layer)},
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

        Callers hold the entry's lock. As with a re-pull, sandboxes running
        on the entry being replaced keep their pins and their open image.
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
            try:
                users = os.listdir(os.path.join(target, _USERS_SUBDIR))
            except OSError:
                users = []
            if os.path.exists(target):
                shutil.rmtree(target, ignore_errors=True)
            os.replace(tmp, target)
        except BaseException:
            shutil.rmtree(tmp, ignore_errors=True)
            raise
        for user in users:
            _mark_image_in_use(target, user)

    def _publish_leaf(
        self,
        key: str,
        work_dir: str,
        layer: Optional[str],
        meta: Dict,
        max_bytes: int,
    ) -> None:
        """Move the finished image into the leaf entry (whose lock the caller holds)."""
        if max_bytes > 0:
            evict_least_recently_used_images(
                self.images_dir,
                max_bytes,
                keep=sanitize_image_name(layer_reference(key)),
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
    if not isinstance(archive, (bytes, bytearray)):
        # Builds run in a worker thread of the sandbox's async actor, where
        # ray.get would block nothing but still warns that it might; waiting
        # on the ref's future is the same wait without the warning.
        archive = archive.future().result()
    with open(path, "wb") as f:
        f.write(archive)


def _builder_version() -> str:
    from ray.experimental.sandbox.image_build import BUILDER_VERSION

    return BUILDER_VERSION
