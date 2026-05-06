import gc
import sys

import pytest

import ray
from ray.data.llm import SGLangEngineProcessorConfig, build_processor


@pytest.fixture(autouse=True)
def _cleanup_sglang_scheduler_actors():
    """Reap orphan sglang_scheduler actors AND their placement groups.

    Two leaks accumulate across tests in the same pytest session:

    1. Ray Data keeps its MapWorker actors warm, so RayEngine's SchedulerActors
       stay alive holding GPUs. SGLangEngineStageUDF.__del__ would release
       them, but __del__ only fires when the outer actor is torn down.

    2. RayEngine's auto-created placement group (the one that owns the
       SchedulerActor GPU bundles) is owned by the job, not by the outer
       MapWorker. Killing the SchedulerActors releases the actor handles
       but the PG stays registered and keeps its bundles' GPUs until the
       job exits. With 3 tests reserving 1 + 2 + 4 GPUs on a 4-GPU cluster,
       the third test's PG request blocks forever.

    Explicitly reap both: kill named sglang_scheduler actors, then remove
    the empty placement groups they lived in.
    """
    yield
    gc.collect()

    # Kill any still-named SchedulerActors. In practice
    # SGLangEngineStageUDF.__del__ will have reaped them already when Ray
    # Data tears down the MapWorker actor pool, but keep this as a
    # belt-and-suspenders pass.
    try:
        named_actors = ray.util.list_named_actors(all_namespaces=True)
    except Exception:
        named_actors = []
    for entry in named_actors:
        name = entry["name"] if isinstance(entry, dict) else entry
        namespace = entry.get("namespace") if isinstance(entry, dict) else None
        if not name.startswith("sglang_scheduler"):
            continue
        try:
            actor = ray.get_actor(name, namespace=namespace)
            ray.kill(actor)
        except Exception:
            pass

    # RayEngine's auto-created placement group is owned by the job, not by
    # the MapWorker, so it lingers after __del__ and keeps its GPU bundles
    # reserved across tests. Ray Data itself uses SPREAD scheduling (no PG),
    # so any live PG with GPU bundles in this test is one of ours — remove
    # every CREATED, GPU-holding PG so the next parametrize case can
    # allocate its own.
    try:
        from ray._raylet import PlacementGroupID
        from ray.util.placement_group import (
            PlacementGroup,
            placement_group_table,
            remove_placement_group,
        )

        for pg_id_hex, pg_info in placement_group_table().items():
            if pg_info.get("state") != "CREATED":
                continue
            bundles = pg_info.get("bundles", {}) or {}
            if not any(b.get("GPU", 0) > 0 for b in bundles.values()):
                continue
            try:
                remove_placement_group(
                    PlacementGroup(PlacementGroupID(bytes.fromhex(pg_id_hex)))
                )
            except Exception:
                pass
    except Exception:
        pass


def test_chat_template():
    chat_template = """
{% if messages[0]['role'] == 'system' %}
    {% set offset = 1 %}
{% else %}
    {% set offset = 0 %}
{% endif %}

{{ bos_token }}
{% for message in messages %}
    {% if (message['role'] == 'user') != (loop.index0 % 2 == offset) %}
        {{ raise_exception('Conversation roles must alternate user/assistant/user/assistant/...') }}
    {% endif %}

    {{ '<|im_start|>' + message['role'] + '\n' + message['content'] | trim + '<|im_end|>\n' }}
{% endfor %}

{% if add_generation_prompt %}
    {{ '<|im_start|>assistant\n' }}
{% endif %}
    """

    processor_config = SGLangEngineProcessorConfig(
        model_source="unsloth/Llama-3.2-1B-Instruct",
        engine_kwargs=dict(
            context_length=2048,
            disable_cuda_graph=True,
            dtype="half",
        ),
        batch_size=16,
        concurrency=1,
        apply_chat_template=True,
        chat_template=chat_template,
        tokenize=True,
        detokenize=True,
    )

    processor = build_processor(
        processor_config,
        preprocess=lambda row: dict(
            messages=[
                {"role": "system", "content": "You are a calculator"},
                {"role": "user", "content": f"{row['id']} ** 3 = ?"},
            ],
            sampling_params=dict(
                temperature=0.3,
                max_new_tokens=50,  # SGLang uses max_new_tokens instead of max_tokens
            ),
        ),
        postprocess=lambda row: {
            "resp": row["generated_text"],
        },
    )

    ds = ray.data.range(60)
    ds = ds.map(lambda x: {"id": x["id"], "val": x["id"] + 5})
    ds = processor(ds)
    ds = ds.materialize()
    outs = ds.take_all()
    assert len(outs) == 60
    assert all("resp" in out for out in outs)


@pytest.mark.parametrize(
    "tp_size,dp_size,concurrency",
    [
        (2, 1, 2),
        (2, 2, 1),
    ],
)
def test_sglang_llama_parallel(tp_size, dp_size, concurrency):
    """Test SGLang with Llama model using different parallelism configurations."""
    runtime_env = {}

    processor_config = SGLangEngineProcessorConfig(
        model_source="unsloth/Llama-3.2-1B-Instruct",
        engine_kwargs=dict(
            context_length=2048,
            tp_size=tp_size,
            dp_size=dp_size,
            dtype="half",
        ),
        runtime_env=runtime_env,
        tokenize=True,
        detokenize=True,
        batch_size=16,
        concurrency=concurrency,
    )

    processor = build_processor(
        processor_config,
        preprocess=lambda row: dict(
            messages=[
                {"role": "system", "content": "You are a calculator"},
                {"role": "user", "content": f"{row['id']} ** 3 = ?"},
            ],
            sampling_params=dict(
                temperature=0.3,
                max_new_tokens=50,
            ),
        ),
        postprocess=lambda row: {
            "resp": row["generated_text"],
        },
    )

    ds = ray.data.range(120)
    ds = ds.map(lambda x: {"id": x["id"], "val": x["id"] + 5})
    ds = processor(ds)
    ds = ds.materialize()

    # Verify results
    outs = ds.take_all()
    assert len(outs) == 120
    assert all("resp" in out for out in outs)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
