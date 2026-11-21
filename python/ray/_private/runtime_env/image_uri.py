import asyncio
import hashlib
import json
import logging
import os
import platform
import shlex
import shutil
import tempfile
import threading
import uuid
from dataclasses import asdict, dataclass, field
from typing import IO, Dict, List, Optional, Tuple

import ray
from ray._private.authentication.authentication_token_setup import (
    _get_default_token_path,
)
from ray._private.authentication.authentication_utils import is_token_auth_enabled
from ray._private.runtime_env.context import (
    RuntimeEnvContext,
    expand_run_option,
)
from ray._private.runtime_env.plugin import RuntimeEnvPlugin
from ray._private.runtime_env.utils import check_output_cmd
from ray._private.utils import get_directory_size_bytes

default_logger = logging.getLogger(__name__)

try:
    import fcntl
except ImportError:  # pragma: no cover - image_uri is only supported on Linux.
    fcntl = None

# Earlier agents do not hold usage leases, so they must use different entries.
_CACHE_SCHEMA_VERSION = 3
_CACHE_URI_PREFIX = "image-pip://"
_DEFAULT_PIP_INSTALL_OPTIONS = [
    "--disable-pip-version-check",
    "--no-cache-dir",
]


def _delete_detached_tree(path: str) -> None:
    """Remove only this detached generation, never rescan live cache names."""
    threading.Thread(
        target=shutil.rmtree,
        args=(path,),
        kwargs={"ignore_errors": True},
        daemon=True,
    ).start()


def _detach_cache_tree(path: str) -> None:
    """Detach a tree while its mutation lock is held, then reclaim it."""
    trash_path = path + f".deleting-{uuid.uuid4().hex}"
    try:
        os.replace(path, trash_path)
    except FileNotFoundError:
        return
    _delete_detached_tree(trash_path)


def _normalize_image_uri(image_uri: str) -> str:
    if image_uri.startswith("podman://"):
        return image_uri[len("podman://") :]
    return image_uri


@dataclass(frozen=True)
class ImageMetadata:
    image_id: str
    image_digest: str
    os: str
    architecture: str
    python_executable: str
    python_version: str
    python_implementation: str
    python_cache_tag: str
    python_soabi: str
    python_platform: str
    python_path: str
    ray_version: str
    ray_commit: str
    worker_path: str


@dataclass
class _ImageCacheEntry:
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    use_lock: Optional[IO] = None
    setups: int = 0
    used: bool = False

    def release_if_unused(self) -> None:
        if not self.used and not self.setups and self.use_lock is not None:
            self.use_lock.close()
            self.use_lock = None


def _canonical_pip_config(runtime_env: "RuntimeEnv") -> Dict:  # noqa: F821
    pip_config = runtime_env.pip_config()
    if runtime_env.has_pip() and not pip_config:
        # pip_config() returns {} for the string form, which names a
        # preinstalled virtualenv on the host and cannot exist in the image.
        raise ValueError(
            "image_uri requires the pip field to be a list of packages or a "
            "pip config dict; a preinstalled virtualenv name is not supported."
        )
    return {
        "packages": [package.strip() for package in pip_config.get("packages", [])],
        "pip_check": bool(pip_config.get("pip_check", False)),
        "pip_version": pip_config.get("pip_version"),
        "pip_install_options": list(
            pip_config.get("pip_install_options", _DEFAULT_PIP_INSTALL_OPTIONS)
        ),
    }


def _get_image_uri_cache_key(
    metadata: ImageMetadata, pip_config: Dict, install_env: Dict[str, str]
) -> str:
    cache_identity = {
        "schema_version": _CACHE_SCHEMA_VERSION,
        "image": {
            "id": metadata.image_id,
            "digest": metadata.image_digest,
            "os": metadata.os,
            "architecture": metadata.architecture,
        },
        "python": {
            "version": metadata.python_version,
            "implementation": metadata.python_implementation,
            "cache_tag": metadata.python_cache_tag,
            "soabi": metadata.python_soabi,
            "platform": metadata.python_platform,
        },
        "ray": {
            "version": metadata.ray_version,
            "commit": metadata.ray_commit,
        },
        "pip": pip_config,
        # Values can affect resolution, but must not be written to the manifest.
        "install_env_sha256": hashlib.sha256(
            json.dumps(install_env, sort_keys=True).encode("utf-8")
        ).hexdigest(),
    }
    return hashlib.sha256(
        json.dumps(cache_identity, sort_keys=True, separators=(",", ":")).encode(
            "utf-8"
        )
    ).hexdigest()


_INSTALL_ENV_PATH = "/tmp/ray-runtime-env-install.json"
_INSTALL_ENV_BOOTSTRAP = """
import json, os, sys
with open(sys.argv[1], encoding="utf-8") as file:
    install_env = json.load(file)
env = os.environ.copy()
env.update(install_env)
os.execve(sys.argv[2], sys.argv[2:], env)
"""


def _write_install_env(path: str, env: Dict[str, str]) -> None:
    """Write literal OS environment entries without podman's line/size limits."""
    for name, value in env.items():
        if not name or "=" in name or "\0" in name or "\0" in value:
            # The value can be secret, so only the name is reported.
            raise ValueError(
                f"Environment variable {name!r} can't be passed to the "
                "image_uri pip installation: invalid OS environment entry."
            )
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "w", encoding="utf-8") as env_file:
        json.dump(env, env_file)


async def _capture_command(cmd: List[str]) -> Tuple[int, str, str]:
    process = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        stdout, stderr = await process.communicate()
    except asyncio.CancelledError:
        process.kill()
        await process.wait()
        raise
    return (
        process.returncode,
        stdout.decode("utf-8", errors="replace"),
        stderr.decode("utf-8", errors="replace"),
    )


async def _remove_container(container_name: str) -> None:
    process = await asyncio.create_subprocess_exec(
        "podman",
        "rm",
        "--force",
        "--ignore",
        container_name,
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.DEVNULL,
    )
    await process.wait()


async def _cleanup_container(container_name: str) -> None:
    cleanup_task = asyncio.create_task(_remove_container(container_name))
    timeout_handle = asyncio.get_running_loop().call_later(10, cleanup_task.cancel)
    try:
        await asyncio.shield(cleanup_task)
    except asyncio.CancelledError:
        if not cleanup_task.cancelled():
            # The caller was cancelled; the removal must still run to completion
            # (or hit its timeout) before the cancellation propagates.
            try:
                await cleanup_task
            except (asyncio.CancelledError, OSError):
                pass
            raise
    except OSError:
        pass
    finally:
        timeout_handle.cancel()


async def _inspect_image(image_uri: str, logger: logging.Logger) -> Tuple[str, Dict]:
    image_reference = _normalize_image_uri(image_uri)
    returncode, _, _ = await _capture_command(
        ["podman", "image", "exists", image_reference]
    )
    if returncode != 0:
        logger.info("Pulling image %s", image_reference)
        await check_output_cmd(
            ["podman", "pull", "--quiet", image_reference], logger=logger
        )

    returncode, stdout, stderr = await _capture_command(
        ["podman", "image", "inspect", image_reference]
    )
    if returncode != 0:
        raise RuntimeError(
            f"Failed to inspect image {image_reference}: {stderr.strip()}"
        )
    inspect_result = json.loads(stdout)
    if not inspect_result:
        raise RuntimeError(f"Podman returned no metadata for image {image_reference}.")
    return image_reference, inspect_result[0]


async def _probe_image(
    image_uri: str, inspect_data: Dict, logger: logging.Logger
) -> ImageMetadata:
    image_id = inspect_data.get("Id") or inspect_data.get("ID")
    if not image_id:
        raise RuntimeError(f"Podman returned no image ID for {image_uri}.")
    image_digest = inspect_data.get("Digest") or image_id

    with tempfile.TemporaryDirectory(prefix="ray-image-uri-probe-") as tmpdir:
        os.chmod(tmpdir, 0o777)
        result_file = os.path.join(tmpdir, "metadata.json")
        probe_script = """
import json
import os
import platform
import sys
import sysconfig
import ray
import ray._private.workers.default_worker as default_worker

metadata = {
    "python_executable": sys.executable,
    "python_version": platform.python_version(),
    "python_implementation": platform.python_implementation(),
    "python_cache_tag": sys.implementation.cache_tag or "",
    "python_soabi": sysconfig.get_config_var("SOABI") or "",
    "python_platform": sysconfig.get_platform(),
    "python_path": os.environ.get("PATH", ""),
    "ray_version": ray.__version__,
    "ray_commit": ray.__commit__,
    "worker_path": default_worker.__file__,
}
with open("/shared/metadata.json", "w", encoding="utf-8") as output:
    json.dump(metadata, output, sort_keys=True)
"""
        container_name = f"ray-runtime-env-probe-{uuid.uuid4().hex}"
        try:
            await check_output_cmd(
                [
                    "podman",
                    "run",
                    "--rm",
                    "--name",
                    container_name,
                    "-v",
                    f"{tmpdir}:/shared:Z",
                    "--entrypoint",
                    "python",
                    image_id,
                    "-c",
                    probe_script,
                ],
                logger=logger,
            )
        except BaseException:
            await _cleanup_container(container_name)
            raise
        if not os.path.isfile(result_file):
            raise RuntimeError(f"Image probe produced no metadata for {image_uri}.")
        with open(result_file, encoding="utf-8") as file:
            probe_data = json.load(file)

    metadata = ImageMetadata(
        image_id=image_id,
        image_digest=image_digest,
        os=inspect_data.get("Os", ""),
        architecture=inspect_data.get("Architecture", ""),
        **probe_data,
    )
    if not metadata.worker_path.endswith(".py"):
        raise ValueError(
            f"Invalid worker path inferred in image {image_uri}: "
            f"{metadata.worker_path}"
        )
    return metadata


def _check_host_compatibility(
    metadata: ImageMetadata,
    image_uri: str,
    require_match: bool,
    logger: logging.Logger,
) -> None:
    """Compare the image's Python and Ray versions against the host.

    The versions are documented as required to match, but image_uri-only
    environments historically ran without enforcement, so a mismatch there
    only warns. The pip cache is keyed on these versions and verified with
    the image interpreter, so with pip a mismatch is an error.
    """
    mismatches = []
    if metadata.python_version != platform.python_version():
        mismatches.append(
            f"Python {metadata.python_version} (host {platform.python_version()})"
        )
    if metadata.ray_version != ray.__version__:
        mismatches.append(f"Ray {metadata.ray_version} (host {ray.__version__})")
    if not mismatches:
        return
    message = (
        f"Image {image_uri} does not match the host: {', '.join(mismatches)}. "
        "The Ray version and Python version in the image must match the "
        "host down to the patch number."
    )
    if require_match:
        raise ValueError(message)
    logger.warning(message)


async def _create_impl(image_uri: str, logger: logging.Logger):
    # Pull image if it doesn't exist
    # Also get path to `default_worker.py` inside the image.
    with tempfile.TemporaryDirectory() as tmpdir:
        os.chmod(tmpdir, 0o777)
        result_file = os.path.join(tmpdir, "worker_path.txt")
        get_worker_path_script = """
import ray._private.workers.default_worker as dw
with open('/shared/worker_path.txt', 'w') as f:
    f.write(dw.__file__)
"""
        cmd = [
            "podman",
            "run",
            "--rm",
            "-v",
            f"{tmpdir}:/shared:Z",
            image_uri,
            "python",
            "-c",
            get_worker_path_script,
        ]

        logger.info("Pulling image %s", image_uri)

        process = await asyncio.create_subprocess_exec(
            *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )

        stdout, stderr = await process.communicate()

        if process.returncode != 0:
            raise RuntimeError(
                f"Podman command failed: cmd={cmd}, returncode={process.returncode}, stdout={stdout.decode()}, stderr={stderr.decode()}"
            )

        if not os.path.exists(result_file):
            raise FileNotFoundError(
                f"Worker path file not created when getting worker path for image {image_uri}"
            )

        with open(result_file, "r") as f:
            worker_path = f.read().strip()

        if not worker_path.endswith(".py"):
            raise ValueError(
                f"Invalid worker path inferred in image {image_uri}: {worker_path}"
            )

        logger.info(f"Inferred worker path in image {image_uri}: {worker_path}")
        return worker_path


def _modify_context_impl(
    image_uri: str,
    worker_path: str,
    run_options: Optional[List[str]],
    context: RuntimeEnvContext,
    logger: logging.Logger,
    ray_tmp_dir: str,
    entrypoint: str = "python",
    extra_args: Optional[List[str]] = None,
    container_env: Optional[Dict[str, str]] = None,
):
    context.override_worker_entrypoint = worker_path

    container_driver = "podman"
    container_command = [
        container_driver,
        "run",
        "-v",
        ray_tmp_dir + ":" + ray_tmp_dir,
        "--cgroup-manager=cgroupfs",
        "--network=host",
        "--pid=host",
        "--ipc=host",
        # NOTE(zcin): Mounted volumes in rootless containers are
        # owned by the user `root`. The user on host (which will
        # usually be `ray` if this is being run in a ray docker
        # image) who started the container is mapped using user
        # namespaces to the user `root` in a rootless container. In
        # order for the Ray Python worker to access the mounted ray
        # tmp dir, we need to use keep-id mode which maps the user
        # as itself (instead of as `root`) into the container.
        # https://www.redhat.com/sysadmin/rootless-podman-user-namespace-modes
        "--userns=keep-id",
        *(extra_args or []),
    ]

    # Ray-owned values for the container. The job's env_vars and the host's
    # RAY_* vars are added when the worker is exec'd; see
    # RuntimeEnvContext._exec_container_worker.
    ray_container_env = dict(container_env or {})

    # A token kept only in a file has no value in podman's environment to pass
    # by name, so mount the file read-only. A token in RAY_AUTH_TOKEN reaches
    # the container by name with the other RAY_* vars.
    if (
        is_token_auth_enabled()
        and "RAY_AUTH_TOKEN" not in context.env_vars
        and "RAY_AUTH_TOKEN" not in os.environ
    ):
        # Ignore the job's env_vars: a job-chosen path would mount any
        # host file the Ray user can read into the container.
        token_path = os.path.abspath(
            os.environ.get("RAY_AUTH_TOKEN_PATH") or _get_default_token_path()
        )
        if os.path.isfile(token_path):
            container_command.extend(["-v", f"{token_path}:{token_path}:ro"])
            ray_container_env["RAY_AUTH_TOKEN_PATH"] = token_path

    run_options = list(run_options or [])
    # Expanded at exec time, but checked here so a malformed item fails
    # runtime_env setup instead of every worker start.
    for run_option in run_options:
        expand_run_option(run_option, {})
    # TODO(chenk008): add resource limit

    # Example:
    # podman run -v /tmp/ray:/tmp/ray
    # --cgroup-manager=cgroupfs --network=host --pid=host --ipc=host
    # --userns=keep-id rayproject/ray:nightly-py39
    logger.info(
        "Starting worker in container with prefix %s %s",
        shlex.join(container_command + run_options),
        image_uri,
    )

    context.container = {
        "command": container_command,
        "run_options": run_options,
        "image": image_uri,
        "entrypoint": entrypoint,
        "env_vars": ray_container_env,
    }


class ImageURIPlugin(RuntimeEnvPlugin):
    """Starts workers in an image with an optional cached pip environment."""

    name = "image_uri"
    requires_uri_cache_validation = True

    @staticmethod
    def get_compatible_keys():
        # _ray_commit and _inject_current_ray are injected by
        # RuntimeEnv.__init__, not supplied by users.
        return {
            "image_uri",
            "pip",
            "config",
            "env_vars",
            "_ray_commit",
            "_inject_current_ray",
        }

    def __init__(self, ray_tmp_dir: str):
        self._ray_tmp_dir = ray_tmp_dir
        self._cache_dir: Optional[str] = None
        self._metadata_by_image_id: Dict[str, ImageMetadata] = {}
        self._probe_locks: Dict[str, asyncio.Lock] = {}
        self._metadata_by_uri: Dict[str, ImageMetadata] = {}
        self._install_env_by_uri: Dict[str, Dict[str, str]] = {}
        self._cache_entries: Dict[str, _ImageCacheEntry] = {}

    def set_resources_dir(self, resources_dir: str) -> None:
        self._cache_dir = os.path.join(resources_dir, "image_uri")
        os.makedirs(self._cache_dir, exist_ok=True)
        if fcntl is None:
            return
        # Remove staging directories left behind by a crashed agent. The
        # per-key lock is taken first so an install currently running in
        # another agent that shares this directory is left alone.
        for entry in os.listdir(self._cache_dir):
            if ".deleting-" in entry:
                # Orphaned trash from an interrupted eviction; no lock is
                # needed because the rename already detached it from its key.
                _delete_detached_tree(os.path.join(self._cache_dir, entry))
                continue
            key, separator, _ = entry.partition(".staging-")
            if not separator:
                continue
            lock_file = self._try_acquire_file_lock(
                os.path.join(self._cache_dir, key + ".lock")
            )
            if lock_file is None:
                continue
            try:
                _detach_cache_tree(os.path.join(self._cache_dir, entry))
            finally:
                fcntl.flock(lock_file, fcntl.LOCK_UN)
                lock_file.close()

    @staticmethod
    def _try_acquire_file_lock(lock_path: str, *, shared: bool = False):
        """Non-blocking flock; returns the open lock file or None if held."""
        if fcntl is None:
            return None
        try:
            lock_file = open(lock_path, "a+")
        except OSError:
            return None
        try:
            mode = fcntl.LOCK_SH if shared else fcntl.LOCK_EX
            fcntl.flock(lock_file, mode | fcntl.LOCK_NB)
        except (BlockingIOError, OSError):
            lock_file.close()
            return None
        return lock_file

    def is_worker_launch_finalizer(self) -> bool:
        return True

    def get_uris(self, runtime_env: "RuntimeEnv") -> List[str]:  # noqa: F821
        # The URI includes image metadata that is only available on the node.
        return []

    @staticmethod
    def _get_install_env(runtime_env: "RuntimeEnv") -> Dict[str, str]:  # noqa: F821
        pip_env_names = {
            "HTTP_PROXY",
            "HTTPS_PROXY",
            "NO_PROXY",
            "REQUESTS_CA_BUNDLE",
            "SSL_CERT_FILE",
            "http_proxy",
            "https_proxy",
            "no_proxy",
        }
        effective_env = {
            key: value
            for key, value in os.environ.items()
            if key.startswith("PIP_") or key in pip_env_names
        }
        # Explicit runtime env variables must be available to build backends and
        # setup hooks during pip install. They also become part of the cache key.
        effective_env.update(runtime_env.env_vars())
        return effective_env

    async def resolve_uris(
        self,
        runtime_env: "RuntimeEnv",  # noqa: F821
        logger: logging.Logger,
    ) -> Optional[List[str]]:
        if not runtime_env.image_uri():
            return None
        if self._cache_dir is None:
            raise RuntimeError("ImageURIPlugin resource directory is not configured.")

        image_reference, inspect_data = await _inspect_image(
            runtime_env.image_uri(), logger
        )
        image_id = inspect_data.get("Id") or inspect_data.get("ID")
        if not image_id:
            raise RuntimeError(f"Podman returned no image ID for {image_reference}.")
        metadata = self._metadata_by_image_id.get(image_id)
        if metadata is None:
            probe_lock = self._probe_locks.setdefault(image_id, asyncio.Lock())
            async with probe_lock:
                metadata = self._metadata_by_image_id.get(image_id)
                if metadata is None:
                    # Probing starts a container, so reuse the result while the
                    # resolved image ID stays the same.
                    metadata = await _probe_image(image_reference, inspect_data, logger)
                    self._metadata_by_image_id[metadata.image_id] = metadata
        _check_host_compatibility(
            metadata, image_reference, runtime_env.has_pip(), logger
        )
        pip_config = _canonical_pip_config(runtime_env)
        install_env = (
            self._get_install_env(runtime_env) if runtime_env.has_pip() else {}
        )
        cache_key = _get_image_uri_cache_key(metadata, pip_config, install_env)
        uri = _CACHE_URI_PREFIX + cache_key
        self._metadata_by_uri[uri] = metadata
        self._install_env_by_uri[uri] = install_env
        return [uri]

    def _get_cache_path(self, uri: str) -> str:
        if self._cache_dir is None or not uri.startswith(_CACHE_URI_PREFIX):
            raise ValueError(f"Invalid image_uri cache URI: {uri}")
        return os.path.join(self._cache_dir, uri[len(_CACHE_URI_PREFIX) :])

    @staticmethod
    def _read_manifest_size(path: str) -> Optional[int]:
        try:
            with open(
                os.path.join(path, "manifest.json"), encoding="utf-8"
            ) as manifest_file:
                size_bytes = json.load(manifest_file).get("size_bytes")
            return (
                size_bytes
                if isinstance(size_bytes, int)
                and not isinstance(size_bytes, bool)
                and size_bytes >= 0
                else None
            )
        except (OSError, ValueError, TypeError, AttributeError):
            return None

    @staticmethod
    def _manifest_is_valid(path: str, uri: str, has_pip: bool) -> bool:
        manifest_path = os.path.join(path, "manifest.json")
        try:
            with open(manifest_path, encoding="utf-8") as manifest_file:
                manifest = json.load(manifest_file)
            size_bytes = manifest.get("size_bytes")
            if (
                manifest.get("schema_version") != _CACHE_SCHEMA_VERSION
                or manifest.get("uri") != uri
                or manifest.get("has_pip") is not has_pip
                or not isinstance(size_bytes, int)
                or isinstance(size_bytes, bool)
                or size_bytes < 0
            ):
                return False
            if has_pip:
                virtualenv_python = os.path.join(path, "virtualenv", "bin", "python")
                # venv normally creates a symlink to the image's interpreter.
                # Its target may not exist on the host that owns this cache.
                if os.path.islink(virtualenv_python):
                    return True
                return os.path.isfile(virtualenv_python) and os.access(
                    virtualenv_python, os.X_OK
                )
            return True
        except (OSError, ValueError, TypeError, AttributeError):
            return False

    @staticmethod
    async def _acquire_file_lock(lock_path: str):
        if fcntl is None:
            raise RuntimeError("image_uri pip caching is only supported on Linux.")
        lock_file = open(lock_path, "a+")
        try:
            while True:
                try:
                    fcntl.flock(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    return lock_file
                except BlockingIOError:
                    await asyncio.sleep(0.1)
        except BaseException:
            lock_file.close()
            raise

    async def _run_in_image(
        self,
        metadata: ImageMetadata,
        staging_path: str,
        final_path: str,
        entrypoint: str,
        args: List[str],
        env_file: Optional[str],
        logger: logging.Logger,
    ) -> str:
        container_name = f"ray-runtime-env-{uuid.uuid4().hex}"
        command = [
            "podman",
            "run",
            "--rm",
            "--name",
            container_name,
            "--network=host",
            "--userns=keep-id",
            "-v",
            f"{staging_path}:{final_path}:rw,z",
        ]
        if env_file is not None:
            command.extend(["-v", f"{env_file}:{_INSTALL_ENV_PATH}:ro,z"])
            args = ["-c", _INSTALL_ENV_BOOTSTRAP, _INSTALL_ENV_PATH, entrypoint, *args]
            entrypoint = metadata.python_executable
        command.extend(["--entrypoint", entrypoint, metadata.image_id, *args])
        try:
            return await check_output_cmd(command, logger=logger)
        except BaseException:
            await _cleanup_container(container_name)
            raise

    async def _prepare_pip_environment(
        self,
        uri: str,
        runtime_env: "RuntimeEnv",  # noqa: F821
        metadata: ImageMetadata,
        staging_path: str,
        final_path: str,
        logger: logging.Logger,
    ) -> None:
        pip_config = _canonical_pip_config(runtime_env)
        requirements_path = os.path.join(staging_path, "requirements.txt")
        with open(requirements_path, "w", encoding="utf-8") as requirements_file:
            for package in pip_config["packages"]:
                requirements_file.write(package + "\n")

        # The install env holds the job's env_vars, so it reaches the
        # container through a file in a private directory. Its values stay off
        # podman's argv, which is logged, and out of podman's own environment,
        # where values such as LD_PRELOAD would affect the host.
        with tempfile.TemporaryDirectory(prefix="ray-image-uri-env-") as env_dir:
            env_file = None
            if self._install_env_by_uri[uri]:
                env_file = os.path.join(env_dir, "env.json")
                _write_install_env(env_file, self._install_env_by_uri[uri])

            async def run(entrypoint: str, args: List[str]) -> str:
                return await self._run_in_image(
                    metadata,
                    staging_path,
                    final_path,
                    entrypoint,
                    args,
                    env_file,
                    logger,
                )

            virtualenv_path = os.path.join(final_path, "virtualenv")
            await run(
                metadata.python_executable,
                [
                    "-m",
                    "venv",
                    "--system-site-packages",
                    "--without-pip",
                    virtualenv_path,
                ],
            )
            virtualenv_python = os.path.join(virtualenv_path, "bin", "python")
            pip_version = pip_config["pip_version"]
            if pip_version:
                await run(
                    virtualenv_python,
                    [
                        "-m",
                        "pip",
                        "install",
                        "--disable-pip-version-check",
                        f"pip{pip_version}",
                    ],
                )
            await run(
                virtualenv_python,
                [
                    "-m",
                    "pip",
                    "install",
                    "-r",
                    os.path.join(final_path, "requirements.txt"),
                    *pip_config["pip_install_options"],
                ],
            )
            if pip_config["pip_check"]:
                await run(
                    virtualenv_python,
                    ["-m", "pip", "check", "--disable-pip-version-check"],
                )
            ray_version_output = await run(
                virtualenv_python,
                ["-c", "import ray; print('RAY_VERSION=' + ray.__version__)"],
            )
        ray_versions = [
            line.removeprefix("RAY_VERSION=")
            for line in ray_version_output.splitlines()
            if line.startswith("RAY_VERSION=")
        ]
        if len(ray_versions) != 1:
            raise RuntimeError("Failed to verify Ray in the image pip environment.")
        ray_version = ray_versions[0]
        if ray_version != metadata.ray_version:
            raise RuntimeError(
                "Changing the Ray version with runtime_env pip is not allowed: "
                f"expected {metadata.ray_version}, found {ray_version}."
            )

    async def create(
        self,
        uri: Optional[str],
        runtime_env: "RuntimeEnv",  # noqa: F821
        context: RuntimeEnvContext,
        logger: logging.Logger,
    ) -> float:
        if not runtime_env.image_uri():
            return 0
        if uri is None or uri not in self._metadata_by_uri:
            raise RuntimeError("Image metadata was not resolved before setup.")
        entry = self._cache_entries.setdefault(uri, _ImageCacheEntry())
        # Count waiters too: a last-reference notification must not release a
        # lease or discard this synchronization object during another setup.
        entry.setups += 1
        try:
            async with entry.lock:
                size_bytes = await self._create_and_acquire(
                    uri, runtime_env, entry, logger
                )
                # Hand off the setup hold to the cache before returning. The
                # agent reconciles references dropped while creation was running.
                entry.used = True
                return size_bytes
        finally:
            entry.setups -= 1
            entry.release_if_unused()

    def on_uri_unused(self, uri: str) -> None:
        entry = self._cache_entries.get(uri)
        if entry is not None:
            entry.used = False
            entry.release_if_unused()

    async def _create_and_acquire(
        self,
        uri: str,
        runtime_env: "RuntimeEnv",  # noqa: F821
        entry: _ImageCacheEntry,
        logger: logging.Logger,
    ) -> int:
        loop = asyncio.get_running_loop()
        final_path = self._get_cache_path(uri)

        def measure() -> int:
            size_bytes = self._read_manifest_size(final_path)
            if size_bytes is None:
                raise RuntimeError(f"Cached image environment has no valid size: {uri}")
            return size_bytes

        if entry.use_lock is not None:
            # A retained lease makes this hit safe without retaking the gate.
            if not self._manifest_is_valid(final_path, uri, runtime_env.has_pip()):
                raise RuntimeError(
                    f"Cached image environment changed while in use: {uri}"
                )
            return await loop.run_in_executor(None, measure)

        lock_file = await self._acquire_file_lock(final_path + ".lock")
        use_lock = None
        try:
            valid = self._manifest_is_valid(final_path, uri, runtime_env.has_pip())
            use_lock = self._try_acquire_file_lock(
                final_path + ".use.lock", shared=valid
            )
            if use_lock is None:
                raise RuntimeError(
                    f"Cannot acquire image environment {uri}: another process is "
                    "using it. An in-use environment cannot be replaced."
                )
            if valid:
                size_bytes = await loop.run_in_executor(None, measure)
            else:
                size_bytes = await self._publish_environment(uri, runtime_env, logger)
                # The gate stays held during the exclusive-to-shared conversion.
                fcntl.flock(use_lock, fcntl.LOCK_SH)
            entry.use_lock = use_lock
            use_lock = None  # Ownership passes to the local cache entry.
            return size_bytes
        finally:
            if use_lock is not None:
                use_lock.close()
            lock_file.close()

    async def _publish_environment(
        self, uri: str, runtime_env: "RuntimeEnv", logger: logging.Logger  # noqa: F821
    ) -> int:
        """Publish under exclusive mutation and usage locks."""
        final_path = self._get_cache_path(uri)
        # Namespace changes stay under the locks, without an await. Deletion
        # threads can only remove detached generations, never a later retry.
        _detach_cache_tree(final_path)
        for stale_path in os.listdir(self._cache_dir):
            if stale_path.startswith(os.path.basename(final_path) + ".staging-") and (
                ".deleting-" not in stale_path
            ):
                _detach_cache_tree(os.path.join(self._cache_dir, stale_path))

        staging_path = final_path + f".staging-{uuid.uuid4().hex}"
        os.makedirs(staging_path)
        try:
            if runtime_env.has_pip():
                await self._prepare_pip_environment(
                    uri,
                    runtime_env,
                    self._metadata_by_uri[uri],
                    staging_path,
                    final_path,
                    logger,
                )
            size_bytes = await asyncio.get_running_loop().run_in_executor(
                None, lambda: get_directory_size_bytes(staging_path)
            )
            manifest = {
                "schema_version": _CACHE_SCHEMA_VERSION,
                "uri": uri,
                "has_pip": runtime_env.has_pip(),
                "image": asdict(self._metadata_by_uri[uri]),
                "pip": _canonical_pip_config(runtime_env),
                "size_bytes": size_bytes,
            }
            manifest_path = os.path.join(staging_path, "manifest.json")
            with open(manifest_path, "w", encoding="utf-8") as manifest_file:
                json.dump(manifest, manifest_file, sort_keys=True)
                manifest_file.flush()
                os.fsync(manifest_file.fileno())
            os.replace(staging_path, final_path)
            directory_fd = os.open(self._cache_dir, os.O_DIRECTORY)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
            return size_bytes
        except BaseException:
            _detach_cache_tree(staging_path)
            raise

    def delete_uri(
        self, uri: str, logger: Optional[logging.Logger] = default_logger
    ) -> Optional[int]:
        entry = self._cache_entries.get(uri)
        if entry is not None and (entry.used or entry.setups):
            return None
        cache_path = self._get_cache_path(uri)
        lock_file = self._try_acquire_file_lock(cache_path + ".lock")
        if lock_file is None:
            # An agent sharing this directory is preparing the same key.
            # Leave the directory in place; the lock file itself is never
            # removed so lock holders stay exclusive.
            logger.warning(
                f"Not deleting {cache_path}: another process holds its lock."
            )
            return None
        use_lock = None
        try:
            use_lock = self._try_acquire_file_lock(cache_path + ".use.lock")
            if use_lock is None:
                return None
            manifest_size = self._read_manifest_size(cache_path)
            size_bytes = manifest_size or 0
            try:
                if os.path.exists(cache_path):
                    if manifest_size is None:
                        size_bytes = get_directory_size_bytes(cache_path)
                    # Renaming is fast; the tree removal happens on a thread
                    # so eviction never blocks the agent's event loop.
                    _detach_cache_tree(cache_path)
            except OSError as error:
                logger.warning(f"Failed to delete {cache_path}: {error}")
                return None
        finally:
            if use_lock is not None:
                use_lock.close()
            lock_file.close()
        self._cache_entries.pop(uri, None)
        self._metadata_by_uri.pop(uri, None)
        self._install_env_by_uri.pop(uri, None)
        return size_bytes

    def modify_context(
        self,
        uris: List[str],
        runtime_env: "RuntimeEnv",  # noqa: F821
        context: RuntimeEnvContext,
        logger: Optional[logging.Logger] = default_logger,
    ):
        if not runtime_env.image_uri():
            return
        if len(uris) != 1 or uris[0] not in self._metadata_by_uri:
            raise RuntimeError("Image metadata was not resolved before worker launch.")

        uri = uris[0]
        metadata = self._metadata_by_uri[uri]
        # Without a cached pip environment there is no cached state to
        # corrupt, so workers use the image reference and podman can re-pull
        # an image that was pruned from the node.
        image = _normalize_image_uri(runtime_env.image_uri())
        entrypoint = metadata.python_executable
        extra_args = ["--rm"]
        container_env = {}
        if runtime_env.has_pip():
            cache_path = self._get_cache_path(uri)
            if not self._manifest_is_valid(cache_path, uri, has_pip=True):
                raise RuntimeError(f"Cached image pip environment is incomplete: {uri}")
            # Workers must run the exact image the environment was built
            # against.
            image = metadata.image_id
            extra_args.extend(["-v", f"{cache_path}:{cache_path}:ro,z"])
            virtualenv_path = os.path.join(cache_path, "virtualenv")
            virtualenv_bin = os.path.join(virtualenv_path, "bin")
            entrypoint = os.path.join(virtualenv_bin, "python")
            # Ray-owned values replace the job's, so keep the job's PATH (or
            # the image's, if the job sets none) after the virtualenv.
            path = context.env_vars.get("PATH", metadata.python_path)
            container_env["VIRTUAL_ENV"] = virtualenv_path
            container_env["PATH"] = (
                f"{virtualenv_bin}:{path}" if path else virtualenv_bin
            )

        _modify_context_impl(
            image,
            metadata.worker_path,
            [],
            context,
            logger,
            self._ray_tmp_dir,
            entrypoint=entrypoint,
            extra_args=extra_args,
            container_env=container_env,
        )


class ContainerPlugin(RuntimeEnvPlugin):
    """Starts worker in container."""

    name = "container"

    def __init__(self, ray_tmp_dir: str):
        self._ray_tmp_dir = ray_tmp_dir

    def is_worker_launch_finalizer(self) -> bool:
        return True

    async def create(
        self,
        uri: Optional[str],
        runtime_env: "RuntimeEnv",  # noqa: F821
        context: RuntimeEnvContext,
        logger: logging.Logger,
    ) -> float:
        if not runtime_env.has_py_container() or not runtime_env.py_container_image():
            return

        self.worker_path = await _create_impl(runtime_env.py_container_image(), logger)

    def modify_context(
        self,
        uris: List[str],
        runtime_env: "RuntimeEnv",  # noqa: F821
        context: RuntimeEnvContext,
        logger: Optional[logging.Logger] = default_logger,
    ):
        if not runtime_env.has_py_container() or not runtime_env.py_container_image():
            return

        if runtime_env.py_container_worker_path():
            logger.warning(
                "You are using `container.worker_path`, but the path to "
                "`default_worker.py` is now automatically detected from the image. "
                "`container.worker_path` is deprecated and will be removed in future "
                "versions."
            )

        _modify_context_impl(
            runtime_env.py_container_image(),
            runtime_env.py_container_worker_path() or self.worker_path,
            runtime_env.py_container_run_options(),
            context,
            logger,
            self._ray_tmp_dir,
        )
