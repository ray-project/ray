"""Test utilities for Ray.

This module contains test utility classes that are distributed with the Ray package
and can be used by external libraries and tests. These utilities must remain in
_common/ (not in tests/) to be accessible in the Ray package distribution.
"""

import asyncio
from collections.abc import Awaitable
from contextlib import contextmanager
import inspect
import os
import time
import traceback
from typing import Any, Callable, Dict, Iterator, List, Optional, Set
import uuid
from enum import Enum


import ray
from ray._common.network_utils import build_address
import ray._private.utils
import ray._common.usage.usage_lib as ray_usage_lib


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
