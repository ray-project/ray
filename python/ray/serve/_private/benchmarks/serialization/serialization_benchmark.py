import asyncio
import pickle
import time
from typing import Any, Callable, List, Optional

import click
import msgpack
from pydantic import BaseModel

from ray._private.serialization import SerializationContext
from ray.cloudpickle import cloudpickle, cloudpickle_fast
from ray.serve._private.benchmarks.common import (
    collect_profile_events,
    run_latency_benchmark,
)
from ray.serve._private.benchmarks.serialization.common import (
    PayloadDataclass,
    PayloadPydantic,
)

_PERCENTILES = [0.5, 0.99]


sc = SerializationContext(None)


def _create_model(cls):
    return PayloadPydantic(
        text="Test output",
        floats=[float(f) for f in range(1, 100)],
        ints=[i for i in range(1, 100)],
        ts=time.time(),
        reason="Success!",
        # error=PayloadPydantic.Error(
        #     msg="No error!",
        #     code=-1,
        #     type="NoErrorError",
        # )
    )


async def run_serializer_benchmark(
    model, serializer: Callable[[Any], bytes], iterations: int
):
    def _serde_loop():
        bs = serializer(model)

        # print(f"Bytes ({len(bs)}): ", bs)

        # new = sc.deserialize_objects([(bs, metadata)], [None])
        # assert new == r

    pd = await run_latency_benchmark(_serde_loop, iterations)

    print("Pydantic latencies: ", pd.describe(percentiles=_PERCENTILES))


@click.command(help="Benchmark serialization latency")
@click.option(
    "--iterations",
    type=int,
    default=1000,
    help="TBA",
)
@click.option(
    "--batch-size",
    type=int,
    default=10,
    help="TBA",
)
@click.option(
    "--payload-type",
    type=str,
    help="TBA",
)
@click.option(
    "--serializer",
    type=str,
    help="TBA",
)
@click.option(
    "--profile-events",
    type=bool,
    default=False,
)
def main(
    iterations: int,
    batch_size: int,
    payload_type: str,
    serializer: str,
    profile_events: bool,
):
    if serializer == "ray":

        def _serialize(obj):
            so = sc.serialize(obj)
            bs, metadata = so.to_bytes(), so.metadata
            return bs

    elif serializer == "cloudpickle":

        def _serialize(obj):
            bs = cloudpickle_fast.dumps(obj)
            return bs

    elif serializer == "pickle":

        def _serialize(obj):
            bs = pickle.dumps(obj)
            return bs

    elif serializer == "msgpack":

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
            bs, metadata = so.to_bytes(), so.metadata
            return bs

    else:
        raise NotImplementedError(serializer)

    if payload_type == "pydantic":
        model = _create_model(PayloadPydantic)
    elif payload_type == "dataclass":
        model = _create_model(PayloadDataclass)
    else:
        raise NotImplementedError(f"Not supported ({payload_type})")

    payload = [model.copy(deep=True) for _ in range(batch_size)]

    routine = run_serializer_benchmark(payload, _serialize, iterations)

    if profile_events:
        routine = collect_profile_events(routine)

    asyncio.run(routine)


if __name__ == "__main__":
    main()
