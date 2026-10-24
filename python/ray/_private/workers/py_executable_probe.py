"""Reports what a worker started with the `py_executable` runtime env would import.

The runtime env agent runs this script with the user's `py_executable`, exactly as
it would run `default_worker.py`, and parses the marker line from its output.
"""

import json
import sys
import sysconfig

PROBE_MARKER = "RAY_PY_EXECUTABLE_PROBE:"


def main() -> None:
    info = {
        "executable": sys.executable,
        "free_threaded": bool(sysconfig.get_config_var("Py_GIL_DISABLED")),
    }
    try:
        from ray._private.utils import compute_version_info

        info["ray_version"], info["python_version"] = compute_version_info()
    except BaseException as e:  # noqa: B036
        # A wrong-ABI extension raises SystemError, a missing install ImportError.
        info["import_error"] = f"{type(e).__name__}: {e}"
    print(PROBE_MARKER + json.dumps(info), flush=True)


if __name__ == "__main__":
    main()
