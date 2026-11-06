"""End-to-end pipelines under the actor-only backend: the sizer sizes real
actor pools and the per-actor output limits plus the escape hatch keep the
pull-based loop from deadlocking.

Sources are in-memory (``from_items``): task-based reads are not driven by
this backend until reads are planned as actor pools (a separate change), so a
``read_*`` / ``range`` source would stall here.
"""
import logging
import sys
import threading
import time
from typing import TYPE_CHECKING, Any, Dict, List

import pytest

import ray
from ray import cloudpickle
from ray.data import ActorPoolStrategy

if TYPE_CHECKING:
    from ray._private.worker import BaseContext

Batch = Dict[str, Any]


class Consumer:
    def __call__(self, batch: Batch) -> Batch:
        return batch


class Slow:
    def __call__(self, batch: Batch) -> Batch:
        time.sleep(0.05)
        return batch


# Pytest imports this module under a name the Ray workers can't import (the
# test dir is not an importable top-level package from a worker's cwd), so the
# UDF classes would fail to deserialize by reference. Force them to pickle by
# value.
cloudpickle.register_pickle_by_value(sys.modules[__name__])


def _items(num_rows: int) -> "ray.data.Dataset":
    """``num_rows`` rows, one per block."""
    return ray.data.from_items(list(range(num_rows)), override_num_blocks=num_rows)


def _output_capped_map(num_rows: int) -> "ray.data.Dataset":
    """An actor-pool map whose per-actor output is capped to a single block,
    forcing aggressive output backpressure upstream of the operator under test.

    The cap is what makes the downstream cases prone to *deadlock* rather than
    mere throttling: the capped actor's output is only freed once a downstream
    operator consumes it, but a downstream that must first buffer several rows
    (an equal split's balance, a large ``batch_size``) can't launch until it
    has them. Without the escape hatch upgrading the cap, the two wait on each
    other forever.
    """
    return _items(num_rows).map_batches(
        Consumer,
        compute=ActorPoolStrategy(size=1, max_num_outputs_per_actor=1),
    )


def _drain_streaming_split(iterators: List[Any]) -> List[int]:
    """Consume every streaming-split iterator concurrently (the split has an
    implicit barrier across its outputs) and return per-split row counts."""
    lengths: List[int] = []
    lock = threading.Lock()

    def _consume(it) -> None:
        count = sum(1 for _ in it.iter_rows())
        with lock:
            lengths.append(count)

    threads = [threading.Thread(target=_consume, args=(it,)) for it in iterators]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    return lengths


def test_autoscaling_pool_grows(
    ray_start_10_cpus_shared: "BaseContext", caplog, propagate_logs
) -> None:
    """A slow actor map with a backlog is sized up by the sizer: the request
    is placed on real nodes and the pipeline still drains every row."""
    ds = _items(400).map_batches(
        Slow, batch_size=2, compute=ActorPoolStrategy(min_size=1, max_size=4)
    )
    with caplog.at_level(logging.DEBUG, logger="ray.data"):
        rows = ds.take_all()
    assert len(rows) == 400

    messages = [r.getMessage() for r in caplog.records]
    assert any(
        "requests MapBatches(Slow): +" in m for m in messages
    ), "the sizer never asked to grow the pool"
    assert any(
        "placement: MapBatches(Slow): +" in m for m in messages
    ), "the sizer never placed the upscale"


def test_map_then_min_rows_map_runs_e2e(
    ray_start_10_cpus_shared: "BaseContext",
) -> None:
    """map1 -> map2(batch_size=64): map2 can't launch until 64 rows are
    buffered, and the capped map1 won't produce more until a downstream task
    consumes -- detection must upgrade map1 so map2 can fill a bundle."""
    ds = _output_capped_map(100).map_batches(
        Consumer, batch_size=64, compute=ActorPoolStrategy(size=1)
    )
    assert len(ds.take_all()) == 100


def test_union_output_capped_input_runs_e2e(
    ray_start_10_cpus_shared: "BaseContext",
) -> None:
    """A union downstream of an output-capped actor pool must not deadlock."""
    ds = _output_capped_map(100).union(_items(50))
    assert len(ds.take_all()) == 150


def test_output_splitter_output_capped_input_runs_e2e(
    ray_start_10_cpus_shared: "BaseContext",
) -> None:
    """An equal streaming split buffers input to balance its outputs, which is
    where it can stall behind an output-capped producer."""
    lengths = _drain_streaming_split(
        _output_capped_map(100).streaming_split(2, equal=True)
    )
    assert sorted(lengths) == [50, 50], lengths


def test_limit_then_output_splitter_runs_e2e(
    ray_start_10_cpus_shared: "BaseContext",
) -> None:
    """map -> limit -> split: the limit launches no tasks and holds no output,
    so the splitter's row requirement has to propagate *through* it back to
    the capped map for detection to upgrade the right operator."""
    lengths = _drain_streaming_split(
        _output_capped_map(100).limit(80).streaming_split(2, equal=True)
    )
    assert sorted(lengths) == [40, 40], lengths


def test_mix_output_capped_input_runs_e2e(
    ray_start_10_cpus_shared: "BaseContext",
) -> None:
    """Mix pulls from whichever input is furthest behind its target ratio and
    waits rather than substituting, so a capped input can stall it."""
    ds = _output_capped_map(100).mix(_items(100), weights=[0.5, 0.5])
    # mix defaults to STOP_ON_LONGEST_DROP, so every row of both inputs lands.
    assert len(ds.take_all()) == 200


def test_union_then_map_then_splitter_runs_e2e(
    ray_start_10_cpus_shared: "BaseContext",
) -> None:
    """map1 -> limit and map2 -> union -> map3 -> split: the splitter's
    requirement is reset at map3 (a task launcher) to its own bundle target,
    which propagates through the union and the limit to the capped map1 and
    independently to map2. Every branch and hop must stay unblocked."""
    unioned = _output_capped_map(100).limit(80).union(_output_capped_map(40))
    mapped = unioned.map_batches(
        Consumer, compute=ActorPoolStrategy(size=1, max_num_outputs_per_actor=1)
    )
    lengths = _drain_streaming_split(mapped.streaming_split(2, equal=True))
    assert sorted(lengths) == [60, 60], lengths


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-x", __file__]))
