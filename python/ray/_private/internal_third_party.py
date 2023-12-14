# Anyone importing this file addes the internal third party files vendored in the Ray
# pip package. Should only be used by the Dashboard and the Runtime Env Agent.
import re
from pathlib import Path
import inspect

INTERNAL_PROCESSES = [
    "ray/_private/runtime_env/agent/main.py",
    "ray/dashboard/dashboard.py",
    "ray/dashboard/agent.py",
]
# It's a shame `pathlib.Path.match` does not support ** until 3.13.
# We have to roll our own regex.
# It's an 2017 issue but only got implemented in 2023...
# https://github.com/python/cpython/issues/73435
TEST_PROCESS_PATTERNS = [
    re.compile(pattern)
    for pattern in (".*/ray/tests/.*", ".*/ray/.*/tests/.*", ".*/ray/release/.*")
]


def check_is_internal_process():
    """
    raises ImportError if this import is illegal. The only leagal importers are the Ray
    internal ones: the Dashboard and the Runtime Env Agent, and dashboard tests.
    """

    last_frame = inspect.stack()[-1]
    path = Path(last_frame.filename)
    print(path, INTERNAL_PROCESSES)
    if any(path.match(expected) for expected in INTERNAL_PROCESSES):
        # internal process
        return
    posix_path = path.as_posix()
    if any(re.match(pattern, posix_path) for pattern in TEST_PROCESS_PATTERNS):
        # Ray tests
        return
    raise ImportError(
        "ray._private.internal_third_party is only for Ray internal use. "
        f"Caller: {path}"
    )


def _configure_path():
    check_is_internal_process()
    import sys
    import os

    sys.path.insert(
        0,
        os.path.join(
            os.path.abspath(os.path.dirname(__file__)), "internal_thirdparty_files"
        ),
    )


_configure_path()
del _configure_path

import aiosignal  # noqa: E402 F401
import aiohttp_cors  # noqa: E402 F401
import aiohttp  # noqa: E402 F401
