import asyncio
import enum
import pickle
import time
from typing import Any, Callable

import click
import msgpack

from ray._private.serialization import SerializationContext
from ray.cloudpickle import cloudpickle_fast
from ray.serve._private.benchmarks.common import (
    collect_profile_events,
    run_latency_benchmark,
)
from ray.serve._private.benchmarks.serialization.common import (
    PayloadDataclass,
    PayloadPydantic,
)


class PayloadType(enum.Enum):
    PYDANTIC = "pydantic"
    DATACLASS = "dataclass"


class SerializerType(enum.Enum):
    RAY = "ray"
    PICKLE = "pickle"
    CLOUDPICKLE = "cloudpickle"
    MSGPACK = "msgpack"


_PERCENTILES = [0.5, 0.99]


sc = SerializationContext(None)


def _create_model(cls):
    return cls(
        text="Test output",
        floats=[float(f) for f in range(1, 100)],
        ints=list(range(1, 100)),
        ts=time.time(),
        reason="Success!",
    )


def _blackhole(o):
    """Placeholder to be used in the benchmark to make sure runtime
    doesn't optimize out unused results"""
    pass


async def run_serializer_benchmark(
    model, serializer: Callable[[Any], bytes], iterations: int
):
    def _serde_loop():
        bs = serializer(model)
        _blackhole(bs)

    pd = await run_latency_benchmark(_serde_loop, iterations)

    print("Latencies (ms):\n", pd.describe(percentiles=_PERCENTILES))


@click.command(help="Benchmark serialization latency")
@click.option(
    "--trials",
    type=int,
    default=1000,
    help="Total number of trials to run in a single benchmark run",
)
@click.option(
    "--batch-size",
    type=int,
    default=10,
    help="Controls how many objects are contained in a serialized batch",
)
@click.option(
    "--payload-type",
    type=PayloadType,
    help="Target type of the payload to be benchmarked (supported: pydantic, "
    "dataclass)",
)
@click.option(
    "--serializer",
    type=SerializerType,
    help="Target type of the serializer to be benchmarked (supported: ray, pickle, "
    "cloudpickle, msgpack)",
)
@click.option(
    "--profile-events",
    type=bool,
    default=False,
)
def main(
    trials: int,
    batch_size: int,
    payload_type: PayloadType,
    serializer: SerializerType,
    profile_events: bool,
):
    if serializer == SerializerType.RAY:

        def _serialize(obj):
            so = sc.serialize(obj)
            bs = so.to_bytes()
            return bs

    elif serializer == SerializerType.CLOUDPICKLE:

        def _serialize(obj):
            bs = cloudpickle_fast.dumps(obj)
            return bs

    elif serializer == SerializerType.PICKLE:

        def _serialize(obj):
            bs = pickle.dumps(obj)
            return bs

    elif serializer == SerializerType.MSGPACK:

        def _dumps(obj):
            bs = msgpack.dumps(obj.__dict__)
            # print(f"Bytes ({len(bs)}): ", bs)
            return bs

        def _loads(bs):
            dict = msgpack.loads(bs)
            return PayloadPydantic(**dict)

        sc._register_cloudpickle_serializer(PayloadPydantic, _dumps, _loads)

        def _serialize(obj):
            so = sc.serialize(obj)
            bs = so.to_bytes()
            return bs

    else:
        raise NotImplementedError(serializer)

    if payload_type == PayloadType.PYDANTIC:
        model = _create_model(PayloadPydantic)
    elif payload_type == PayloadType.DATACLASS:
        model = _create_model(PayloadDataclass)
    else:
        raise NotImplementedError(f"Not supported ({payload_type})")

    payload = [model.copy(deep=True) for _ in range(batch_size)]

    routine = run_serializer_benchmark(payload, _serialize, trials)

    if profile_events:
        routine = collect_profile_events(routine)

    asyncio.run(routine)


if __name__ == "__main__":
    main()
