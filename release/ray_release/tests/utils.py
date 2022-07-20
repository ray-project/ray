from collections import Counter
from typing import Any

UNIT_TEST_PROJECT_ID = "prj_HqxHjwtn2uRtzR3DW6AmBYZh"

UNIT_TEST_CLOUD_ID = "cld_4F7k8814aZzGG8TNUGPKnc"


class UnitTestError(RuntimeError):
    pass


def fail_always(*a, **kw):
    raise UnitTestError()


def fail_once(result: Any):
    class _Failer:
        def __init__(self):
            self.failed = False

        def __call__(self, *args, **kwargs):
            if not self.failed:
                self.failed = True
                raise UnitTestError()
            return result

    return _Failer()


class APIDict(dict):
    __slots__ = ()
    __getattr__ = dict.__getitem__
    __setattr__ = dict.__setattr__


class MockSDK:
    def __init__(self):
        self.returns = {}
        self.call_counter = Counter()

    def reset(self):
        self.returns = {}
        self.call_counter = Counter()

    def __getattr__(self, item):
        self.call_counter[item] += 1
        result = self.returns.get(item)
        if callable(result):
            return result
        else:
            return lambda *a, **kw: result
