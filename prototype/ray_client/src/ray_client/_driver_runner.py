"""Cluster-side driver entrypoint packaged with every `run_function_as_driver` job.

Fork this file to change how the cluster driver initializes Ray, loads its
inputs, or reports its output (e.g., to Redis, S3/GCS, or an internal service).
"""

import asyncio
import atexit
import base64
import importlib.util
import inspect
import json
import math
import os
import sys
import traceback
from typing import Any, Optional, Tuple

RESULT_LINE_PREFIX = "__RAY_CLIENT_RESULT__:"
PAYLOAD_ENV_VAR = "RAY_CLIENT_PAYLOAD_B64"
PAYLOAD_URI_ENV_VAR = "RAY_CLIENT_PAYLOAD_URI"
SUBMISSION_ID_ENV_VAR = "RAY_CLIENT_SUBMISSION_ID"
NAMESPACE_ENV_VAR = "RAY_CLIENT_NAMESPACE"
PAYLOAD_FILENAME = "_ray_client_payload.json"
USER_FN_FILENAME = "_ray_client_user_fn.py"
JOB_CONFIG_ENV_VAR = "RAY_JOB_CONFIG_JSON_ENV_VAR"


def _dup_stdout_fd() -> Optional[int]:
    try:
        return os.dup(1)
    except OSError:
        return None


def _load_payload() -> Tuple[dict, Optional[str], Optional[str]]:
    """Loads `{fn_name, args, kwargs, captured_globals}` and scrubs internal env vars.

    Small payloads arrive in PAYLOAD_ENV_VAR; larger ones ship as PAYLOAD_FILENAME
    in a separate `py_modules` package (or in `working_dir`). Internal env vars and
    any per-call payload `py_modules` URI are removed from both `os.environ` and
    Ray's serialized job config *before* `ray.init()` so Ray does not propagate
    them to workers.
    """
    b64 = os.environ.pop(PAYLOAD_ENV_VAR, None)
    payload_uri = os.environ.pop(PAYLOAD_URI_ENV_VAR, None)
    submission_id = os.environ.pop(SUBMISSION_ID_ENV_VAR, None)
    namespace = os.environ.pop(NAMESPACE_ENV_VAR, None)

    job_config_json = os.environ.get(JOB_CONFIG_ENV_VAR)
    if job_config_json:
        cfg = json.loads(job_config_json)
        runtime_env = cfg.get("runtime_env", {})
        env_vars = runtime_env.get("env_vars", {})
        for key in (
            PAYLOAD_ENV_VAR,
            PAYLOAD_URI_ENV_VAR,
            SUBMISSION_ID_ENV_VAR,
            NAMESPACE_ENV_VAR,
        ):
            env_vars.pop(key, None)
        if payload_uri and isinstance(runtime_env.get("py_modules"), list):
            runtime_env["py_modules"] = [
                u for u in runtime_env["py_modules"] if u != payload_uri
            ]
            if not runtime_env["py_modules"]:
                runtime_env.pop("py_modules", None)
        os.environ[JOB_CONFIG_ENV_VAR] = json.dumps(cfg)

    if b64 is not None:
        return json.loads(base64.b64decode(b64)), namespace, submission_id

    for d in ("", *sys.path):
        candidate = os.path.join(d, PAYLOAD_FILENAME) if d else PAYLOAD_FILENAME
        if os.path.isfile(candidate):
            with open(candidate, encoding="utf-8") as f:
                return json.load(f), namespace, submission_id
    raise FileNotFoundError(
        f"Could not find {PAYLOAD_FILENAME} in working_dir or PYTHONPATH."
    )


def _validate_strict_json(value: Any, path: str = "Return value") -> None:
    if value is None or value is True or value is False:
        return
    val_type = type(value)
    if val_type is int or val_type is str:
        return
    if val_type is float:
        if not math.isfinite(value):
            raise TypeError(
                f"{path} contains non-finite float {value!r}; NaN and Infinity "
                "are not valid JSON."
            )
        return
    if val_type is list:
        for idx, item in enumerate(value):
            _validate_strict_json(item, f"{path}[{idx}]")
        return
    if val_type is dict:
        for k, v in value.items():
            if type(k) is not str:
                raise TypeError(
                    f"{path} has non-string dict key {k!r} of type "
                    f"'{type(k).__name__}'; JSON object keys must be str."
                )
            _validate_strict_json(v, f"{path}[{k!r}]")
        return
    if isinstance(value, tuple):
        raise TypeError(
            f"{path} is a tuple, which JSON would convert to a list; "
            "convert tuples to lists explicitly."
        )
    raise TypeError(
        f"Driver function returned a non-JSON-serializable value at {path} of type "
        f"'{val_type.__name__}'. Return strict JSON data (dict, list, str, int, "
        "float, bool, or None); ObjectRefs and actor handles cannot cross the "
        "client boundary."
    )


def _emit_result_line(
    payload: dict, submission_id: Optional[str], result_fd: Optional[int]
) -> None:
    if submission_id:
        payload = {"submission_id": submission_id, **payload}
    encoded = base64.b64encode(json.dumps(payload).encode("utf-8")).decode("ascii")
    tag = f"{submission_id}:" if submission_id else ""
    line = f"{RESULT_LINE_PREFIX}{tag}{encoded}\n"
    sys.stdout.flush()
    sys.stderr.flush()
    if result_fd is not None:
        try:
            os.write(result_fd, line.encode("utf-8"))
            return
        except OSError:
            pass
    sys.stdout.write(line)
    sys.stdout.flush()


def _install_ray_init_shim(namespace: Optional[str]) -> None:
    import ray

    orig_init = ray.init
    is_initialized = getattr(ray, "is_initialized", lambda: False)
    last_ctx = []

    def _shimmed_init(*args: Any, **kwargs: Any) -> Any:
        addr = args[0] if args else kwargs.get("address")
        if isinstance(addr, str) and "://" in addr:
            raise RuntimeError(
                f"ray.init({addr!r}) cannot use a client URI scheme inside an "
                "in-cluster driver function; call ray.init() without a 'ray://' "
                "address."
            )
        runtime_env = kwargs.get("runtime_env")
        if isinstance(runtime_env, dict) and "working_dir" in runtime_env:
            runtime_env = dict(runtime_env)
            runtime_env.pop("working_dir", None)
            kwargs["runtime_env"] = runtime_env
        if is_initialized():
            return last_ctx[0] if last_ctx else None
        if namespace:
            kwargs.setdefault("namespace", namespace)
        ctx = orig_init(*args, **kwargs)
        last_ctx[:] = [ctx]
        return ctx

    ray.init = _shimmed_init
    for attr in ("cluster_resources", "available_resources", "nodes"):
        orig_fn = getattr(ray, attr, None)
        if callable(orig_fn):

            def _auto_init_state(*a: Any, _fn: Any = orig_fn, **kw: Any) -> Any:
                if not is_initialized():
                    ray.init()
                return _fn(*a, **kw)

            setattr(ray, attr, _auto_init_state)


def _load_user_fn(fn_name: str, captured_globals: dict):
    import ray

    spec = importlib.util.spec_from_file_location(
        "__main__", os.path.abspath(USER_FN_FILENAME)
    )
    module = importlib.util.module_from_spec(spec)
    # Captured client-side constants must exist before `def` runs. Executing the
    # module as `__main__` (matching `ray job submit`) ensures classes and
    # dataclasses defined inside tasks/actors resolve `sys.modules['__main__']`
    # on workers while still pickling by value.
    module.__dict__.update(captured_globals)
    sys.modules["__main__"] = module
    sys.modules["_ray_client_user_fn"] = module
    spec.loader.exec_module(module)
    ray.cloudpickle.register_pickle_by_value(module)
    return getattr(module, fn_name)


def _shutdown_ray() -> None:
    if "ray" not in sys.modules:
        return
    _ray = sys.modules["ray"]
    if getattr(_ray, "is_initialized", lambda: False)():
        getattr(_ray, "shutdown", lambda: None)()


def main() -> None:
    result_fd = _dup_stdout_fd()
    submission_id: Optional[str] = None
    exit_code = 0
    try:
        payload, namespace, submission_id = _load_payload()
        _install_ray_init_shim(namespace)
        fn = _load_user_fn(payload["fn_name"], payload["captured_globals"])
        result = fn(*payload["args"], **payload["kwargs"])
        if inspect.iscoroutine(result):
            result = asyncio.run(result)
        _validate_strict_json(result, "Return value")
        outcome = {"ok": True, "result": result}
    except BaseException as exc:
        if isinstance(exc, SystemExit) and exc.code in (None, 0):
            outcome = {"ok": True, "result": None}
        else:
            exit_code = 1
            tb = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
            sys.stderr.write(tb)
            sys.stderr.flush()
            outcome = {
                "ok": False,
                "error": {
                    "exc_type": type(exc).__name__,
                    "exc_message": str(exc),
                    "traceback": tb,
                },
            }

    try:
        atexit._run_exitfuncs()
    except BaseException:
        pass
    _shutdown_ray()
    _emit_result_line(outcome, submission_id, result_fd)
    if result_fd is not None:
        try:
            os.close(result_fd)
        except OSError:
            pass
    os._exit(exit_code)


if __name__ == "__main__":
    main()
