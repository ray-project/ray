import threading

import pytest

import ray
from ray.data.llm import build_processor, vLLMEngineProcessorConfig


@pytest.fixture(autouse=True)
def cleanup_ray_resources():
    """Automatically cleanup Ray resources between tests to prevent conflicts."""
    yield
    ray.shutdown()


@pytest.mark.parametrize(
    "tp_size,pp_size",
    [
        (2, 4),
        (4, 2),
    ],
)
def test_vllm_multi_node(tp_size, pp_size):
    config = vLLMEngineProcessorConfig(
        model_source="facebook/opt-1.3b",
        engine_kwargs=dict(
            enable_prefix_caching=True,
            enable_chunked_prefill=True,
            max_num_batched_tokens=4096,
            pipeline_parallel_size=pp_size,
            tensor_parallel_size=tp_size,
            distributed_executor_backend="ray",
        ),
        tokenize=False,
        detokenize=False,
        concurrency=1,
        batch_size=64,
        apply_chat_template=False,
    )

    processor = build_processor(
        config,
        preprocess=lambda row: dict(
            prompt=f"You are a calculator. {row['id']} ** 3 = ?",
            sampling_params=dict(
                temperature=0.3,
                max_tokens=20,
                detokenize=True,
            ),
        ),
        postprocess=lambda row: dict(
            resp=row["generated_text"],
        ),
    )

    ds = ray.data.range(60)
    ds = processor(ds)
    ds = ds.materialize()

    outs = ds.take_all()
    assert len(outs) == 60
    assert all("resp" in out for out in outs)


def test_vllm_engine_no_pg_double_reservation():
    """Regression test for GPU placement-group double-reservation.

    A single ``tensor_parallel_size=N`` replica must reserve exactly ``N`` GPUs
    in one placement group. Previously ``HighMemoryIssueDetector`` invoked the
    engine stage's ``ray_remote_args_fn``, leaking an extra PG that reserved
    ``N`` idle GPUs and could starve other replicas (stuck "pending creation").
    """
    tp_size = 2
    config = vLLMEngineProcessorConfig(
        model_source="facebook/opt-1.3b",
        engine_kwargs=dict(
            tensor_parallel_size=tp_size,
            distributed_executor_backend="ray",
            max_num_batched_tokens=4096,
            gpu_memory_utilization=0.6,
        ),
        tokenize=False,
        detokenize=False,
        concurrency=1,
        batch_size=16,
        apply_chat_template=False,
    )
    processor = build_processor(
        config,
        preprocess=lambda row: dict(
            prompt=f"You are a calculator. {row['id']} ** 3 = ?",
            sampling_params=dict(temperature=0.3, max_tokens=20, detokenize=True),
        ),
        postprocess=lambda row: dict(resp=row["generated_text"]),
    )

    ray.init(address="auto", ignore_reinit_error=True)
    pre_existing = set(ray.util.placement_group_table())
    peak = {"pgs": 0, "gpus": 0}
    stop = threading.Event()

    def monitor():
        # Track the peak count of CREATED GPU placement groups (and GPUs they
        # reserve) created by this run.
        while not stop.is_set():
            pgs = gpus = 0
            for pg_id, info in ray.util.placement_group_table().items():
                if pg_id in pre_existing or info.get("state") != "CREATED":
                    continue
                n = sum(b.get("GPU", 0) for b in (info.get("bundles") or {}).values())
                if n:
                    pgs += 1
                    gpus += n
            peak["pgs"], peak["gpus"] = max(peak["pgs"], pgs), max(peak["gpus"], gpus)
            stop.wait(1.0)

    thread = threading.Thread(target=monitor, daemon=True)
    thread.start()
    try:
        ds = ray.data.range(60)
        ds = processor(ds).materialize()
        outs = ds.take_all()
    finally:
        stop.set()
        thread.join(timeout=5)

    assert len(outs) == 60
    assert all("resp" in out for out in outs)
    assert peak["gpus"] == tp_size and peak["pgs"] == 1, (
        f"tp={tp_size} replica reserved {peak['gpus']} GPUs across {peak['pgs']} "
        f"placement groups; expected {tp_size} GPUs in 1 PG (PG double-reservation)"
    )
