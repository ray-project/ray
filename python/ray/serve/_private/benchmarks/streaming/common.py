import abc
import asyncio
import enum
import logging
import time
from typing import Tuple, Union

import numpy as np

from ray.actor import ActorHandle
from ray.runtime_env import RuntimeEnv
from ray.serve._private.benchmarks.common import Blackhole, run_throughput_benchmark
from ray.serve._private.benchmarks.serialization.common import PayloadPydantic
from ray.serve.handle import DeploymentHandle

GRPC_DEBUG_RUNTIME_ENV = RuntimeEnv(
    env_vars={"GRPC_TRACE": "http", "GRPC_VERBOSITY": "debug"},
)


class IOMode(enum.Enum):
    SYNC = "SYNC"
    ASYNC = "ASYNC"


class Endpoint:
    def __init__(self, tokens_per_request: int):
        self._tokens_per_request = tokens_per_request
        # Switch off logging to minimize its impact
        logging.getLogger("ray").setLevel(logging.WARNING)
        logging.getLogger("ray.serve").setLevel(logging.WARNING)

    def stream(self):
        payload = PayloadPydantic(
            text="Test output",
            floats=[float(f) for f in range(1, 100)],
            ints=list(range(1, 100)),
            ts=time.time(),
            reason="Success!",
        )

        for i in range(self._tokens_per_request):
            yield payload

    async def aio_stream(self):
        payload = PayloadPydantic(
            text="Test output",
            floats=[float(f) for f in range(1, 100)],
            ints=list(range(1, 100)),
            ts=time.time(),
            reason="Success!",
        )

        for i in range(self._tokens_per_request):
            yield payload


class Caller(Blackhole):
    def __init__(
        self,
        downstream: Union[ActorHandle, DeploymentHandle],
        *,
        mode: IOMode,
        tokens_per_request: int,
        batch_size: int,
        num_trials: int,
        trial_runtime: float,
    ):
        self._h = downstream
        self._mode = mode
        self._tokens_per_request = tokens_per_request
        self._batch_size = batch_size
        self._num_trials = num_trials
        self._trial_runtime = trial_runtime
        self._durations = []

        # Switch off logging to minimize its impact
        logging.getLogger("ray").setLevel(logging.WARNING)
        logging.getLogger("ray.serve").setLevel(logging.WARNING)

    def _get_remote_method(self):
        if self._mode == IOMode.SYNC:
            return self._h.stream
        elif self._mode == IOMode.ASYNC:
            return self._h.aio_stream
        else:
            raise NotImplementedError(f"Streaming mode not supported ({self._mode})")

    @abc.abstractmethod
    async def _consume_single_stream(self):
        pass

    async def _do_single_batch(self):
        durations = await asyncio.gather(
            *[
                self._execute(self._consume_single_stream)
                for _ in range(self._batch_size)
            ]
        )

        self._durations.extend(durations)

    async def _execute(self, fn):
        start = time.monotonic()
        await fn()
        dur_s = time.monotonic() - start
        return dur_s * 1000  # ms

    async def run_benchmark(self) -> Tuple[float, float]:
        coro = run_throughput_benchmark(
            fn=self._do_single_batch,
            multiplier=self._batch_size * self._tokens_per_request,
            num_trials=self._num_trials,
            trial_runtime=self._trial_runtime,
        )
        # total_runtime = await collect_profile_events(coro)
        total_runtime = await coro

        p50, p75, p99 = np.percentile(self._durations, [50, 75, 99])

        print(f"Individual request quantiles:\n\tP50={p50}\n\tP75={p75}\n\tP99={p99}")

        return total_runtime
