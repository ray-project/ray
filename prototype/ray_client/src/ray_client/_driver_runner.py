"""Cluster-side driver entrypoint packaged with every `run_function_as_driver` job.

Fork this file to change how the cluster driver initializes Ray, loads its
inputs, or reports its output (e.g., to Redis, S3/GCS, or an internal service).
"""

import asyncio
import base64
import importlib.util
import inspect
import json
import os
import sys
import traceback

RESULT_LINE_PREFIX = "__RAY_CLIENT_RESULT__:"
PAYLOAD_ENV_VAR = "RAY_CLIENT_PAYLOAD_B64"
NAMESPACE_ENV_VAR = "RAY_CLIENT_NAMESPACE"
PAYLOAD_FILENAME = "_ray_client_payload.json"
USER_FN_FILENAME = "_ray_client_user_fn.py"
JOB_CONFIG_ENV_VAR = "RAY_JOB_CONFIG_JSON_ENV_VAR"


def _load_payload() -> dict:
    """Loads `{fn_name, args, kwargs, captured_globals}` from env or working_dir.

    Small payloads arrive in PAYLOAD_ENV_VAR; larger ones ship as PAYLOAD_FILENAME
    inside the working_dir package. The env var is removed from both os.environ and
    Ray's serialized job config *before* ray.init() so Ray does not propagate it to
    every worker's runtime_env.
    """
    b64 = os.environ.pop(PAYLOAD_ENV_VAR, None)
    job_config_json = os.environ.get(JOB_CONFIG_ENV_VAR)
    if job_config_json:
        cfg = json.loads(job_config_json)
        cfg.get("runtime_env", {}).get("env_vars", {}).pop(PAYLOAD_ENV_VAR, None)
        os.environ[JOB_CONFIG_ENV_VAR] = json.dumps(cfg)
    if b64 is not None:
        return json.loads(base64.b64decode(b64))
    with open(PAYLOAD_FILENAME, encoding="utf-8") as f:
        return json.load(f)


def _emit_result_line(payload: dict) -> None:
    encoded = base64.b64encode(json.dumps(payload).encode("utf-8")).decode("ascii")
    sys.stdout.write(f"{RESULT_LINE_PREFIX}{encoded}\n")
    sys.stdout.flush()


def _init_ray() -> None:
    import ray

    # All ray_client drivers join one namespace so named/detached actors created
    # by one call are visible to the next via `ray.get_actor(name)`.
    ray.init(namespace=os.environ.get(NAMESPACE_ENV_VAR, "ray_client"))
    # User functions commonly call `ray.init()` themselves out of habit; make that
    # a no-op instead of a "called ray.init twice" error.
    orig_init = ray.init
    ray.init = lambda *a, **kw: orig_init(*a, **{"ignore_reinit_error": True, **kw})


def _load_user_fn(fn_name: str, captured_globals: dict):
    import ray

    spec = importlib.util.spec_from_file_location(
        "_ray_client_user_fn", USER_FN_FILENAME
    )
    module = importlib.util.module_from_spec(spec)
    # Captured client-side constants must exist before the `def` runs because
    # default argument values are evaluated at definition time.
    module.__dict__.update(captured_globals)
    sys.modules[module.__name__] = module
    spec.loader.exec_module(module)
    # If `fn` itself is shipped to a worker, pickle it by value so the worker
    # does not re-import this module without the injected captured_globals.
    ray.cloudpickle.register_pickle_by_value(module)
    return getattr(module, fn_name)


def main() -> None:
    try:
        payload = _load_payload()
        _init_ray()
        fn = _load_user_fn(payload["fn_name"], payload["captured_globals"])
        result = fn(*payload["args"], **payload["kwargs"])
        if inspect.isawaitable(result):
            result = asyncio.run(result)
        try:
            json.dumps(result, allow_nan=False)
        except (TypeError, OverflowError, ValueError) as exc:
            raise TypeError(
                "Driver function returned a non-JSON-serializable value of type "
                f"'{type(result).__name__}'. Return JSON data (or None); ObjectRefs "
                "and actor handles cannot cross the client boundary."
            ) from exc
    except BaseException as exc:
        tb = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
        sys.stderr.write(tb)
        sys.stderr.flush()
        _emit_result_line(
            {
                "ok": False,
                "error": {
                    "exc_type": type(exc).__name__,
                    "exc_message": str(exc),
                    "traceback": tb,
                },
            }
        )
        sys.exit(1)
    _emit_result_line({"ok": True, "result": result})


if __name__ == "__main__":
    main()
