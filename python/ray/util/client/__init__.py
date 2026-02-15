import logging
import os
import threading
from typing import Any, Dict, List, Optional, Tuple

import ray._private.ray_constants as ray_constants
from ray._private.client_mode_hook import (
    _explicitly_disable_client_mode,
    _explicitly_enable_client_mode,
)
from ray._private.ray_logging import setup_logger
from ray._private.utils import check_version_info
from ray.job_config import JobConfig
from ray.util.annotations import DeveloperAPI

logger = logging.getLogger(__name__)


def _apply_uv_hook_for_client(
    runtime_env: Optional[Dict[str, Any]],
) -> Optional[Dict[str, Any]]:
    """Apply UV runtime env hook on client side before connection.

    UV (https://docs.astral.sh/uv/) is a modern Python package manager that
    manages dependencies via pyproject.toml and uv.lock files. This function
    detects when the client is running under 'uv run' and automatically
    propagates the UV configuration to cluster workers so they can install
    the same dependencies.

    How it works:
        1. Detects 'uv run' in the parent process tree
        2. Extracts UV command-line arguments (e.g., --python, --locked)
        3. Sets py_executable to 'uv run [args]' in runtime_env
        4. Workers will use this UV command to install dependencies

    Precedence rules:
        - If user provides py_executable, UV hook is skipped entirely to avoid
          unintended side effects (e.g., auto-setting working_dir)
        - User-provided working_dir is preserved when UV hook runs
        - Other runtime_env settings are merged with UV config

    Feature flag:
        Controlled by RAY_ENABLE_UV_RUN_RUNTIME_ENV constant (default: enabled)

    Args:
        runtime_env: The runtime environment dict to potentially modify.
            Can be None if no runtime_env was specified.

    Returns:
        Modified runtime_env dict with UV configuration if detected,
        otherwise the original runtime_env unchanged. Returns None if
        input was None.

    Raises:
        RuntimeError: If UV environment is detected but configuration is invalid
            (e.g., pyproject.toml not in working_dir, conflicting runtime_env).
            Validation errors fail fast to provide clear feedback.

    Note:
        ImportError and other environmental errors are caught and logged,
        allowing connection to proceed without UV propagation.

    Example:
        Client running under: uv run --python 3.11 my_script.py

        >>> runtime_env = {"working_dir": "/tmp/myapp"}
        >>> result = _apply_uv_hook_for_client(runtime_env)
        >>> result
        {'working_dir': '/tmp/myapp', 'py_executable': 'uv run --python 3.11'}

    See Also:
        - Issue: https://github.com/ray-project/ray/issues/57991
        - UV docs: https://docs.astral.sh/uv/
    """
    try:
        if not ray_constants.RAY_ENABLE_UV_RUN_RUNTIME_ENV:
            return runtime_env

        # If user provided py_executable, skip UV hook entirely to avoid side effects
        # (e.g., auto-setting working_dir which triggers unwanted directory upload)
        if runtime_env and "py_executable" in runtime_env:
            logger.debug(
                "User-provided py_executable found, skipping UV hook to avoid "
                "unintended runtime_env modifications"
            )
            return runtime_env

        # Import hook here (not at module level) to:
        # 1. Avoid circular import issues with ray._private modules
        # 2. Only load UV hook code when feature flag is enabled
        from ray._private.runtime_env.uv_runtime_env_hook import hook

        result = hook(runtime_env)
        if "py_executable" in result:
            # UV environment was detected and applied by the hook
            logger.debug(
                f"UV environment detected for Ray Client: "
                f"py_executable={result['py_executable']}"
            )
            return result
    except (ImportError, KeyError, ValueError, OSError) as e:
        # Expected errors: module not available, internal hook errors, file system issues
        # Log and proceed without UV propagation
        logger.warning(
            f"Failed to apply UV runtime env hook for Ray Client: {e}. "
            "UV environment will not be propagated to workers."
        )
    except Exception:
        # Unexpected errors - log with full traceback but don't fail connection
        logger.exception(
            "Unexpected error in UV runtime env hook for Ray Client. "
            "UV environment will not be propagated to workers."
        )

    return runtime_env


class _ClientContext:
    def __init__(self):
        from ray.util.client.api import _ClientAPI

        self.api = _ClientAPI()
        self.client_worker = None
        self._server = None
        self._connected_with_init = False
        self._inside_client_test = False

    def connect(
        self,
        conn_str: str,
        job_config: JobConfig = None,
        secure: bool = False,
        metadata: List[Tuple[str, str]] = None,
        connection_retries: int = 3,
        namespace: str = None,
        *,
        ignore_version: bool = False,
        _credentials: Optional["grpc.ChannelCredentials"] = None,  # noqa: F821
        ray_init_kwargs: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Connect the Ray Client to a server.

        Args:
            conn_str: Connection string, in the form "[host]:port"
            job_config: The job config of the server.
            secure: Whether to use a TLS secured gRPC channel
            metadata: gRPC metadata to send on connect
            connection_retries: number of connection attempts to make
            ignore_version: whether to ignore Python or Ray version mismatches.
                This should only be used for debugging purposes.

        Returns:
            Dictionary of connection info, e.g., {"num_clients": 1}.
        """
        # Delay imports until connect to avoid circular imports.
        from ray.util.client.worker import Worker

        if self.client_worker is not None:
            if self._connected_with_init:
                return
            raise Exception("ray.init() called, but ray client is already connected")
        if not self._inside_client_test:
            # If we're calling a client connect specifically and we're not
            # currently in client mode, ensure we are.
            _explicitly_enable_client_mode()
        if namespace is not None:
            job_config = job_config or JobConfig()
            job_config.set_ray_namespace(namespace)

        logging_level = ray_constants.LOGGER_LEVEL
        logging_format = ray_constants.LOGGER_FORMAT

        if ray_init_kwargs is None:
            ray_init_kwargs = {}

        # Apply UV hook client-side before connection.
        # UV detection must happen on client side where 'uv run' process exists.
        # See: https://github.com/ray-project/ray/issues/57991
        #
        # Runtime env can come from two sources:
        # 1. ray_init_kwargs["runtime_env"] - directly passed to connect()
        # 2. job_config.runtime_env - passed via JobConfig object
        # We need to handle both sources and update them appropriately after UV hook.
        runtime_env = ray_init_kwargs.get("runtime_env")
        if runtime_env is None and job_config and job_config.runtime_env is not None:
            runtime_env = job_config.runtime_env

        runtime_env = _apply_uv_hook_for_client(runtime_env)

        if runtime_env is not None:
            # Update both ray_init_kwargs and job_config with UV modifications.
            # This is necessary because _server_init() reads runtime_env from
            # job_config.runtime_env, not from ray_init_kwargs["runtime_env"].
            ray_init_kwargs["runtime_env"] = runtime_env
            if job_config:
                job_config.set_runtime_env(runtime_env)

        # NOTE(architkulkarni): Custom env_hook is not supported with Ray Client.
        # However, UV hook is now applied client-side above.
        ray_init_kwargs["_skip_env_hook"] = True

        if ray_init_kwargs.get("logging_level") is not None:
            logging_level = ray_init_kwargs["logging_level"]
        if ray_init_kwargs.get("logging_format") is not None:
            logging_format = ray_init_kwargs["logging_format"]

        setup_logger(logging_level, logging_format)

        try:
            self.client_worker = Worker(
                conn_str,
                secure=secure,
                _credentials=_credentials,
                metadata=metadata,
                connection_retries=connection_retries,
            )
            self.api.worker = self.client_worker
            self.client_worker._server_init(job_config, ray_init_kwargs)
            conn_info = self.client_worker.connection_info()
            self._check_versions(conn_info, ignore_version)
            self._register_serializers()
            return conn_info
        except Exception:
            self.disconnect()
            raise

    def _register_serializers(self):
        """Register the custom serializer addons at the client side.

        The server side should have already registered the serializers via
        regular worker's serialization_context mechanism.
        """
        import ray.util.serialization_addons
        from ray.util.serialization import StandaloneSerializationContext

        ctx = StandaloneSerializationContext()
        ray.util.serialization_addons.apply(ctx)

    def _check_versions(self, conn_info: Dict[str, Any], ignore_version: bool) -> None:
        # conn_info has "python_version" and "ray_version" so it can be used to compare.
        ignore_version = ignore_version or ("RAY_IGNORE_VERSION_MISMATCH" in os.environ)
        check_version_info(
            conn_info,
            "Ray Client",
            raise_on_mismatch=not ignore_version,
            python_version_match_level="minor",
        )

    def disconnect(self):
        """Disconnect the Ray Client."""
        from ray.util.client.api import _ClientAPI

        if self.client_worker is not None:
            self.client_worker.close()
        self.api = _ClientAPI()
        self.client_worker = None

    # remote can be called outside of a connection, which is why it
    # exists on the same API layer as connect() itself.
    def remote(self, *args, **kwargs):
        """remote is the hook stub passed on to replace `ray.remote`.

        This sets up remote functions or actors, as the decorator,
        but does not execute them.

        Args:
            args: opaque arguments
            kwargs: opaque keyword arguments
        """
        return self.api.remote(*args, **kwargs)

    def __getattr__(self, key: str):
        if self.is_connected():
            return getattr(self.api, key)
        elif key in ["is_initialized", "_internal_kv_initialized"]:
            # Client is not connected, thus Ray is not considered initialized.
            return lambda: False
        else:
            raise Exception(
                "Ray Client is not connected. Please connect by calling `ray.init`."
            )

    def is_connected(self) -> bool:
        if self.client_worker is None:
            return False
        return self.client_worker.is_connected()

    def init(self, *args, **kwargs):
        if self._server is not None:
            raise Exception("Trying to start two instances of ray via client")
        import ray.util.client.server.server as ray_client_server

        server_handle, address_info = ray_client_server.init_and_serve(
            "127.0.0.1", 50051, *args, **kwargs
        )
        self._server = server_handle.grpc_server
        self.connect("127.0.0.1:50051")
        self._connected_with_init = True
        return address_info

    def shutdown(self, _exiting_interpreter=False):
        self.disconnect()
        import ray.util.client.server.server as ray_client_server

        if self._server is None:
            return
        ray_client_server.shutdown_with_server(self._server, _exiting_interpreter)
        self._server = None


# All connected context will be put here
# This struct will be guarded by a lock for thread safety
_all_contexts = set()
_lock = threading.Lock()

# This is the default context which is used when allow_multiple is not True
_default_context = _ClientContext()


@DeveloperAPI
class RayAPIStub:
    """This class stands in as the replacement API for the `import ray` module.

    Much like the ray module, this mostly delegates the work to the
    _client_worker. As parts of the ray API are covered, they are piped through
    here or on the client worker API.
    """

    def __init__(self):
        self._cxt = threading.local()
        self._cxt.handler = _default_context
        self._inside_client_test = False

    def get_context(self):
        try:
            return self._cxt.__getattribute__("handler")
        except AttributeError:
            self._cxt.handler = _default_context
            return self._cxt.handler

    def set_context(self, cxt):
        old_cxt = self.get_context()
        if cxt is None:
            self._cxt.handler = _ClientContext()
        else:
            self._cxt.handler = cxt
        return old_cxt

    def is_default(self):
        return self.get_context() == _default_context

    def connect(self, *args, **kw_args):
        self.get_context()._inside_client_test = self._inside_client_test
        conn = self.get_context().connect(*args, **kw_args)
        global _lock, _all_contexts
        with _lock:
            _all_contexts.add(self._cxt.handler)
        return conn

    def disconnect(self, *args, **kw_args):
        global _lock, _all_contexts, _default_context
        with _lock:
            if _default_context == self.get_context():
                for cxt in _all_contexts:
                    cxt.disconnect(*args, **kw_args)
                _all_contexts = set()
            else:
                self.get_context().disconnect(*args, **kw_args)
            if self.get_context() in _all_contexts:
                _all_contexts.remove(self.get_context())
            if len(_all_contexts) == 0:
                _explicitly_disable_client_mode()

    def remote(self, *args, **kwargs):
        return self.get_context().remote(*args, **kwargs)

    def __getattr__(self, name):
        return self.get_context().__getattr__(name)

    def is_connected(self, *args, **kwargs):
        return self.get_context().is_connected(*args, **kwargs)

    def init(self, *args, **kwargs):
        ret = self.get_context().init(*args, **kwargs)
        global _lock, _all_contexts
        with _lock:
            _all_contexts.add(self._cxt.handler)
        return ret

    def shutdown(self, *args, **kwargs):
        global _lock, _all_contexts
        with _lock:
            if _default_context == self.get_context():
                for cxt in _all_contexts:
                    cxt.shutdown(*args, **kwargs)
                _all_contexts = set()
            else:
                self.get_context().shutdown(*args, **kwargs)
            if self.get_context() in _all_contexts:
                _all_contexts.remove(self.get_context())
            if len(_all_contexts) == 0:
                _explicitly_disable_client_mode()


ray = RayAPIStub()


@DeveloperAPI
def num_connected_contexts():
    """Return the number of client connections active."""
    global _lock, _all_contexts
    with _lock:
        return len(_all_contexts)


# Someday we might add methods in this module so that someone who
# tries to `import ray_client as ray` -- as a module, instead of
# `from ray_client import ray` -- as the API stub
# still gets expected functionality. This is the way the ray package
# worked in the past.
#
# This really calls for PEP 562: https://www.python.org/dev/peps/pep-0562/
# But until Python 3.6 is EOL, here we are.
