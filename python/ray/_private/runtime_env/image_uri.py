import asyncio
import hashlib
import json
import logging
import os
import platform
import shutil
import tempfile
import threading
import uuid
from dataclasses import asdict, dataclass
from typing import Dict, List, Optional, Tuple

import ray
from ray._private.runtime_env.context import RuntimeEnvContext
from ray._private.runtime_env.plugin import RuntimeEnvPlugin
from ray._private.runtime_env.utils import check_output_cmd
from ray._private.utils import get_directory_size_bytes

default_logger = logging.getLogger(__name__)

try:
    import fcntl
except ImportError:  # pragma: no cover - image_uri is only supported on Linux.
    fcntl = None

_CACHE_SCHEMA_VERSION = 1
_CACHE_URI_PREFIX = "image-pip://"
_DEFAULT_PIP_INSTALL_OPTIONS = [
    "--disable-pip-version-check",
    "--no-cache-dir",
]


def _podman_run_base_command(ray_tmp_dir: str) -> List[str]:
    """The podman flags shared by ContainerPlugin and ImageURIPlugin workers."""
    return [
        "podman",
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
    ]


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
):
    context.override_worker_entrypoint = worker_path

    container_command = _podman_run_base_command(ray_tmp_dir)

    # Environment variables to set in container
    env_vars = dict()

    # Propagate all host environment variables that have the prefix "RAY_"
    # This should include RAY_RAYLET_PID
    for env_var_name, env_var_value in os.environ.items():
        if env_var_name.startswith("RAY_"):
            env_vars[env_var_name] = env_var_value

    # Support for runtime_env['env_vars']
    env_vars.update(context.env_vars)

    # Set environment variables
    for env_var_name, env_var_value in env_vars.items():
        container_command.append("--env")
        container_command.append(f"{env_var_name}='{env_var_value}'")

    # The RAY_JOB_ID environment variable is needed for the default worker.
    # It won't be set at the time setup() is called, but it will be set
    # when worker command is executed, so we use RAY_JOB_ID=$RAY_JOB_ID
    # for the container start command
    container_command.append("--env")
    container_command.append("RAY_JOB_ID=$RAY_JOB_ID")

    if run_options:
        container_command.extend(run_options)
    # TODO(chenk008): add resource limit
    container_command.append("--entrypoint")
    container_command.append("python")
    container_command.append(image_uri)

    # Example:
    # podman run -v /tmp/ray:/tmp/ray
    # --cgroup-manager=cgroupfs --network=host --pid=host --ipc=host
    # --userns=keep-id --env RAY_RAYLET_PID=23478 --env RAY_JOB_ID=$RAY_JOB_ID
    # --entrypoint python rayproject/ray:nightly-py39
    container_command_str = " ".join(container_command)
    logger.info(f"Starting worker in container with prefix {container_command_str}")

    context.py_executable = container_command_str


class ImageURIPlugin(RuntimeEnvPlugin):
    """Starts workers in an image with an optional cached pip environment."""

    name = "image_uri"

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
        self._metadata_by_uri: Dict[str, ImageMetadata] = {}
        self._install_env_by_uri: Dict[str, Dict[str, str]] = {}
        self._create_locks: Dict[str, asyncio.Lock] = {}

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
                threading.Thread(
                    target=shutil.rmtree,
                    args=(os.path.join(self._cache_dir, entry),),
                    kwargs={"ignore_errors": True},
                    daemon=True,
                ).start()
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
                shutil.rmtree(os.path.join(self._cache_dir, entry), ignore_errors=True)
            finally:
                fcntl.flock(lock_file, fcntl.LOCK_UN)
                lock_file.close()

    @staticmethod
    def _try_acquire_file_lock(lock_path: str):
        """Non-blocking flock; returns the open lock file or None if held."""
        if fcntl is None:
            return None
        try:
            lock_file = open(lock_path, "a+")
        except OSError:
            return None
        try:
            fcntl.flock(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
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
        effective_env.update(
            {
                key: value
                for key, value in runtime_env.env_vars().items()
                if key.startswith("PIP_") or key in pip_env_names
            }
        )
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
            return size_bytes if isinstance(size_bytes, int) else None
        except (OSError, ValueError, TypeError, AttributeError):
            return None

    @staticmethod
    def _manifest_is_valid(path: str, uri: str) -> bool:
        manifest_path = os.path.join(path, "manifest.json")
        try:
            with open(manifest_path, encoding="utf-8") as manifest_file:
                manifest = json.load(manifest_file)
            return (
                manifest.get("schema_version") == _CACHE_SCHEMA_VERSION
                and manifest.get("uri") == uri
            )
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
        install_env: Dict[str, str],
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
        child_env = os.environ.copy()
        child_env.update(install_env)
        for key in sorted(install_env):
            # Podman reads the value from its own environment. This keeps secrets
            # out of the logged command line.
            command.extend(["--env", key])
        command.extend(["--entrypoint", entrypoint, metadata.image_id, *args])
        try:
            return await check_output_cmd(command, logger=logger, env=child_env)
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
        install_env = self._install_env_by_uri[uri]
        requirements_path = os.path.join(staging_path, "requirements.txt")
        with open(requirements_path, "w", encoding="utf-8") as requirements_file:
            for package in pip_config["packages"]:
                requirements_file.write(package + "\n")

        virtualenv_path = os.path.join(final_path, "virtualenv")
        await self._run_in_image(
            metadata,
            staging_path,
            final_path,
            metadata.python_executable,
            [
                "-m",
                "venv",
                "--system-site-packages",
                "--without-pip",
                virtualenv_path,
            ],
            install_env,
            logger,
        )
        virtualenv_python = os.path.join(virtualenv_path, "bin", "python")
        pip_version = pip_config["pip_version"]
        if pip_version:
            await self._run_in_image(
                metadata,
                staging_path,
                final_path,
                virtualenv_python,
                [
                    "-m",
                    "pip",
                    "install",
                    "--disable-pip-version-check",
                    f"pip{pip_version}",
                ],
                install_env,
                logger,
            )
        await self._run_in_image(
            metadata,
            staging_path,
            final_path,
            virtualenv_python,
            [
                "-m",
                "pip",
                "install",
                "-r",
                os.path.join(final_path, "requirements.txt"),
                *pip_config["pip_install_options"],
            ],
            install_env,
            logger,
        )
        if pip_config["pip_check"]:
            await self._run_in_image(
                metadata,
                staging_path,
                final_path,
                virtualenv_python,
                ["-m", "pip", "check", "--disable-pip-version-check"],
                install_env,
                logger,
            )
        ray_version_output = await self._run_in_image(
            metadata,
            staging_path,
            final_path,
            virtualenv_python,
            ["-c", "import ray; print('RAY_VERSION=' + ray.__version__)"],
            install_env,
            logger,
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
        if uri not in self._create_locks:
            self._create_locks[uri] = asyncio.Lock()

        # The virtualenv can hold hundreds of thousands of files; run the
        # directory walks and removals in a thread to keep the agent's event
        # loop responsive.
        loop = asyncio.get_running_loop()
        final_path = self._get_cache_path(uri)

        def measure() -> float:
            size_bytes = self._read_manifest_size(final_path)
            if size_bytes is not None:
                return size_bytes
            return get_directory_size_bytes(final_path)

        async with self._create_locks[uri]:
            if self._manifest_is_valid(final_path, uri):
                return await loop.run_in_executor(None, measure)

            lock_path = final_path + ".lock"
            lock_file = await self._acquire_file_lock(lock_path)
            try:
                if self._manifest_is_valid(final_path, uri):
                    return await loop.run_in_executor(None, measure)

                def remove_previous_trees() -> None:
                    if os.path.exists(final_path):
                        shutil.rmtree(final_path, ignore_errors=True)
                    for stale_path in os.listdir(self._cache_dir):
                        if stale_path.startswith(
                            os.path.basename(final_path) + ".staging-"
                        ):
                            shutil.rmtree(
                                os.path.join(self._cache_dir, stale_path),
                                ignore_errors=True,
                            )

                await loop.run_in_executor(None, remove_previous_trees)

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
                    size_bytes = await loop.run_in_executor(
                        None, lambda: get_directory_size_bytes(staging_path)
                    )
                    manifest = {
                        "schema_version": _CACHE_SCHEMA_VERSION,
                        "uri": uri,
                        "image": asdict(self._metadata_by_uri[uri]),
                        "pip": _canonical_pip_config(runtime_env),
                        # Recorded so cache hits and eviction never have to
                        # walk the tree.
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
                except BaseException:
                    try:
                        await asyncio.shield(
                            loop.run_in_executor(
                                None,
                                lambda: shutil.rmtree(staging_path, ignore_errors=True),
                            )
                        )
                    except asyncio.CancelledError:
                        pass  # The executor thread still finishes the removal.
                    raise
            finally:
                fcntl.flock(lock_file, fcntl.LOCK_UN)
                lock_file.close()

            return size_bytes

    def delete_uri(
        self, uri: str, logger: Optional[logging.Logger] = default_logger
    ) -> int:
        cache_path = self._get_cache_path(uri)
        # The URI leaves this cache's tracking regardless of whether the
        # directory is removed below, so its recorded contribution to the
        # cache size must be subtracted either way; otherwise a skipped
        # deletion followed by a re-add double-counts the directory.
        manifest_size = self._read_manifest_size(cache_path)
        size_bytes = manifest_size or 0
        lock_file = self._try_acquire_file_lock(cache_path + ".lock")
        if lock_file is None:
            # An agent sharing this directory is preparing the same key.
            # Leave the directory in place; the lock file itself is never
            # removed so lock holders stay exclusive.
            logger.warning(
                f"Not deleting {cache_path}: another process holds its lock."
            )
        else:
            try:
                if os.path.exists(cache_path):
                    if manifest_size is None:
                        size_bytes = get_directory_size_bytes(cache_path)
                    # Renaming is fast; the tree removal happens on a thread
                    # so eviction never blocks the agent's event loop.
                    trash_path = cache_path + f".deleting-{uuid.uuid4().hex}"
                    os.replace(cache_path, trash_path)
                    threading.Thread(
                        target=shutil.rmtree,
                        args=(trash_path,),
                        kwargs={"ignore_errors": True},
                        daemon=True,
                    ).start()
            except OSError as error:
                logger.warning(f"Failed to delete {cache_path}: {error}")
            finally:
                fcntl.flock(lock_file, fcntl.LOCK_UN)
                lock_file.close()
        self._create_locks.pop(uri, None)
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
        entrypoint = metadata.python_executable
        mounts = []
        container_env = {}
        path_prefix = None
        if runtime_env.has_pip():
            cache_path = self._get_cache_path(uri)
            if not self._manifest_is_valid(cache_path, uri):
                raise RuntimeError(f"Cached image pip environment is incomplete: {uri}")
            mounts.append(
                {
                    "source": cache_path,
                    "target": cache_path,
                    "read_only": True,
                    "options": "z",
                }
            )
            virtualenv_path = os.path.join(cache_path, "virtualenv")
            entrypoint = os.path.join(virtualenv_path, "bin", "python")
            container_env["VIRTUAL_ENV"] = virtualenv_path
            path_prefix = os.path.join(virtualenv_path, "bin")

        context.override_worker_entrypoint = metadata.worker_path
        context.py_executable = entrypoint
        context.container = {
            "command": [*_podman_run_base_command(self._ray_tmp_dir), "--rm"],
            # With a cached pip environment, workers must run the exact image
            # the environment was built against. Without one there is no
            # cached state to corrupt, so workers use the image reference and
            # podman can re-pull an image that was pruned from the node.
            "image": metadata.image_id
            if runtime_env.has_pip()
            else _normalize_image_uri(runtime_env.image_uri()),
            "entrypoint": entrypoint,
            "mounts": mounts,
            "env_vars": container_env,
            "path_prefix": path_prefix,
            "default_path": metadata.python_path,
        }
        logger.info(
            "Configured worker image %s with cache URI %s.",
            runtime_env.image_uri(),
            uri,
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
