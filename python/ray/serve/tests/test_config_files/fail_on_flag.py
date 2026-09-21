"""Test fixture: a deployment whose constructor fails on demand.

The failure is switched on either by the app builder argument ``fail`` (a
rebuild-class change) or by the ``FAIL_ON_INIT`` environment variable set through
``ray_actor_options.runtime_env`` (a replica-restart-class change).
"""
import os

from ray import serve


@serve.deployment
class FailOnFlag:
    def __init__(self, fail: bool):
        if fail or os.environ.get("FAIL_ON_INIT") == "1":
            raise RuntimeError("constructor failure requested by the test")

    def __call__(self, *args):
        return "ok"


def build(args):
    return FailOnFlag.bind(args.get("fail", False))
