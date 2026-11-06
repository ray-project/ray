"""Test fixture: raises on first FLAKY_BUILD_FAIL_COUNT imports, then succeeds.

A counter file persists state across the fresh worker processes that
``build_serve_application``'s ``max_calls=1`` decorator spawns on each retry.
"""

import os

from ray import serve

_COUNTER_FILE = os.environ["FLAKY_BUILD_COUNTER_FILE"]
_FAIL_COUNT = int(os.environ.get("FLAKY_BUILD_FAIL_COUNT", "3"))

with open(_COUNTER_FILE, "r") as f:
    _attempts = int(f.read().strip() or "0")
with open(_COUNTER_FILE, "w") as f:
    f.write(str(_attempts + 1))

if _attempts < _FAIL_COUNT:
    raise RuntimeError(f"flaky build failure on attempt {_attempts + 1}")


@serve.deployment
class FlakyApp:
    def __call__(self):
        return "ok"


node = FlakyApp.bind()
