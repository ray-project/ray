"""Test utilities for Ray.

This module contains test utility classes that are distributed with the Ray package
and can be used by external libraries and tests. These utilities must remain in
_common/ (not in tests/) to be accessible in the Ray package distribution.
"""

import asyncio
import inspect
import os
import threading
import time
import traceback
import uuid
from collections.abc import Awaitable
from contextlib import contextmanager
from enum import Enum
from typing import Any, Callable, Dict, Iterator, List, Optional, Set

import ray
import ray._common.usage.usage_lib as ray_usage_lib
import ray._private.utils
from ray._common.network_utils import build_address


@ray.remote(num_cpus=0)
class SignalActor:
    """A Ray actor for coordinating test execution through signals.

    Useful for testing async coordination, waiting for specific states,
    and synchronizing multiple actors or tasks in tests.
    """

    def __init__(self):
        self.ready_event = asyncio.Event()
        self.num_waiters = 0

    def send(self, clear: bool = False):
        self.ready_event.set()
        if clear:
            self.ready_event.clear()

    async def wait(self, should_wait: bool = True):
        if should_wait:
            self.num_waiters += 1
            await self.ready_event.wait()
            self.num_waiters -= 1

    async def cur_num_waiters(self) -> int:
        return self.num_waiters


@ray.remote(num_cpus=0)
class Semaphore:
    """A Ray actor implementing a semaphore for test coordination.

    Useful for testing resource limiting, concurrency control,
    and coordination between multiple actors or tasks.
    """

    def __init__(self, value: int = 1):
        self._sema = asyncio.Semaphore(value=value)

    async def acquire(self):
        await self._sema.acquire()

    async def release(self):
        self._sema.release()

    async def locked(self) -> bool:
        return self._sema.locked()


__all__ = ["SignalActor", "Semaphore"]


def wait_for_condition(
    condition_predictor: Callable[..., bool],
    timeout: float = 10,
    retry_interval_ms: float = 100,
    raise_exceptions: bool = False,
    **kwargs: Any,
):
    """Wait until a condition is met or time out with an exception.

    Args:
        condition_predictor: A function that predicts the condition.
        timeout: Maximum timeout in seconds.
        retry_interval_ms: Retry interval in milliseconds.
        raise_exceptions: If true, exceptions that occur while executing
            condition_predictor won't be caught and instead will be raised.
        **kwargs: Arguments to pass to the condition_predictor.

    Returns:
        None: Returns when the condition is met.

    Raises:
        RuntimeError: If the condition is not met before the timeout expires.
    """
    start = time.time()
    last_ex = None
    while time.time() - start <= timeout:
        try:
            if condition_predictor(**kwargs):
                return
        except Exception:
            if raise_exceptions:
                raise
            last_ex = ray._private.utils.format_error_message(traceback.format_exc())
        time.sleep(retry_interval_ms / 1000.0)
    message = "The condition wasn't met before the timeout expired."
    if last_ex is not None:
        message += f" Last exception: {last_ex}"
    raise RuntimeError(message)


async def async_wait_for_condition(
    condition_predictor: Callable[..., Awaitable[bool]],
    timeout: float = 10,
    retry_interval_ms: float = 100,
    **kwargs: Any,
):
    """Wait until a condition is met or time out with an exception.

    Args:
        condition_predictor: A function that predicts the condition.
        timeout: Maximum timeout in seconds.
        retry_interval_ms: Retry interval in milliseconds.
        **kwargs: Arguments to pass to the condition_predictor.

    Returns:
        None: Returns when the condition is met.

    Raises:
        RuntimeError: If the condition is not met before the timeout expires.
    """
    start = time.time()
    last_ex = None
    while time.time() - start <= timeout:
        try:
            if inspect.iscoroutinefunction(condition_predictor):
                if await condition_predictor(**kwargs):
                    return
            else:
                if condition_predictor(**kwargs):
                    return
        except Exception as ex:
            last_ex = ex
        await asyncio.sleep(retry_interval_ms / 1000.0)
    message = "The condition wasn't met before the timeout expired."
    if last_ex is not None:
        message += f" Last exception: {last_ex}"
    raise RuntimeError(message)


@contextmanager
def simulate_s3_bucket(
    port: int = 5002,
    region: str = "us-west-2",
) -> Iterator[str]:
    """Context manager that simulates an S3 bucket and yields the URI.

    Args:
        port: The port of the localhost endpoint where S3 is being served.
        region: The S3 region.

    Yields:
        str: URI for the simulated S3 bucket.
    """
    from moto.server import ThreadedMotoServer

    old_env = os.environ
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"

    s3_server = f"http://{build_address('localhost', port)}"
    server = ThreadedMotoServer(port=port)
    server.start()
    url = f"s3://{uuid.uuid4().hex}?region={region}&endpoint_override={s3_server}"
    yield url
    server.stop()
    os.environ = old_env


class TelemetryCallsite(Enum):
    DRIVER = "driver"
    ACTOR = "actor"
    TASK = "task"


def _get_library_usages() -> Set[str]:
    return set(
        ray_usage_lib.get_library_usages_to_report(
            ray.experimental.internal_kv.internal_kv_get_gcs_client()
        )
    )


def _get_extra_usage_tags() -> Dict[str, str]:
    return ray_usage_lib.get_extra_usage_tags_to_report(
        ray.experimental.internal_kv.internal_kv_get_gcs_client()
    )


def check_library_usage_telemetry(
    use_lib_fn: Callable[[], None],
    *,
    callsite: TelemetryCallsite,
    expected_library_usages: List[Set[str]],
    expected_extra_usage_tags: Optional[Dict[str, str]] = None,
):
    """Helper for writing tests to validate library usage telemetry.

    `use_lib_fn` is a callable that will be called from the provided callsite.
    After calling it, the telemetry data to export will be validated against
    expected_library_usages and expected_extra_usage_tags.
    """
    assert len(_get_library_usages()) == 0, _get_library_usages()

    if callsite == TelemetryCallsite.DRIVER:
        use_lib_fn()
    elif callsite == TelemetryCallsite.ACTOR:

        @ray.remote
        class A:
            def __init__(self):
                use_lib_fn()

        a = A.remote()
        ray.get(a.__ray_ready__.remote())
    elif callsite == TelemetryCallsite.TASK:

        @ray.remote
        def f():
            use_lib_fn()

        ray.get(f.remote())
    else:
        assert False, f"Unrecognized callsite: {callsite}"

    library_usages = _get_library_usages()
    extra_usage_tags = _get_extra_usage_tags()

    assert library_usages in expected_library_usages, library_usages
    if expected_extra_usage_tags:
        assert all(
            [extra_usage_tags[k] == v for k, v in expected_extra_usage_tags.items()]
        ), extra_usage_tags


class FakeTimer:
    def __init__(self, start_time: Optional[float] = None):
        self._lock = threading.Lock()
        self.reset(start_time=start_time)

    def reset(self, start_time: Optional[float] = None):
        with self._lock:
            if start_time is None:
                start_time = time.time()
            self._curr = start_time

    def time(self) -> float:
        return self._curr

    def advance(self, by: float):
        with self._lock:
            self._curr += by

    def realistic_sleep(self, amt: float):
        with self._lock:
            self._curr += amt + 0.001


def is_named_tuple(cls):
    """Return True if cls is a namedtuple and False otherwise."""
    b = cls.__bases__
    if len(b) != 1 or b[0] is not tuple:
        return False
    f = getattr(cls, "_fields", None)
    if not isinstance(f, tuple):
        return False
    return all(type(n) is str for n in f)


def assert_tensors_equivalent(obj1, obj2):
    """
    Recursively compare objects with special handling for torch.Tensor.

    Tensors are considered equivalent if:
      - Same dtype and shape
      - Same device type (e.g., both 'cpu' or both 'cuda'), index ignored
      - Values are equal (or close for floats)
    """
    import torch

    if isinstance(obj1, torch.Tensor) and isinstance(obj2, torch.Tensor):
        # 1. dtype
        assert obj1.dtype == obj2.dtype, f"dtype mismatch: {obj1.dtype} vs {obj2.dtype}"
        # 2. shape
        assert obj1.shape == obj2.shape, f"shape mismatch: {obj1.shape} vs {obj2.shape}"
        # 3. device type must match (cpu/cpu or cuda/cuda), ignore index
        assert (
            obj1.device.type == obj2.device.type
        ), f"Device type mismatch: {obj1.device} vs {obj2.device}"

        # 4. Compare values safely on CPU
        t1_cpu = obj1.cpu()
        t2_cpu = obj2.cpu()
        if obj1.dtype.is_floating_point or obj1.dtype.is_complex:
            assert torch.allclose(
                t1_cpu, t2_cpu, atol=1e-6, rtol=1e-5
            ), "Floating-point tensors not close"
        else:
            assert torch.equal(t1_cpu, t2_cpu), "Integer/bool tensors not equal"
        return

    # Type must match
    if type(obj1) is not type(obj2):
        raise AssertionError(f"Type mismatch: {type(obj1)} vs {type(obj2)}")

    # Handle namedtuples
    if is_named_tuple(type(obj1)):
        assert len(obj1) == len(obj2)
        for a, b in zip(obj1, obj2):
            assert_tensors_equivalent(a, b)
    elif isinstance(obj1, dict):
        assert obj1.keys() == obj2.keys()
        for k in obj1:
            assert_tensors_equivalent(obj1[k], obj2[k])
    elif isinstance(obj1, (list, tuple)):
        assert len(obj1) == len(obj2)
        for a, b in zip(obj1, obj2):
            assert_tensors_equivalent(a, b)
    elif hasattr(obj1, "__dict__") and hasattr(obj2, "__dict__"):
        # Compare user-defined objects by their public attributes
        keys1 = {
            k
            for k in obj1.__dict__.keys()
            if not k.startswith("_ray_") and k != "_pytype_"
        }
        keys2 = {
            k
            for k in obj2.__dict__.keys()
            if not k.startswith("_ray_") and k != "_pytype_"
        }
        assert keys1 == keys2, f"Object attribute keys differ: {keys1} vs {keys2}"
        for k in keys1:
            assert_tensors_equivalent(obj1.__dict__[k], obj2.__dict__[k])
    else:
        # Fallback for primitives: int, float, str, bool, etc.
        assert obj1 == obj2, f"Non-tensor values differ: {obj1} vs {obj2}"
