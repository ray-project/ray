from ray.llm._internal.batch.stages.vllm_engine_stage import vLLMEngineStage
from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy


def test_vllm_engine_stage_basic():
    stage = vLLMEngineStage(
        fn_constructor_kwargs=dict(
            model="facebook/opt-125m",
            engine_kwargs=dict(
                tensor_parallel_size=4,
                pipeline_parallel_size=2,
                distributed_executor_backend="ray",
            ),
            task_type="generate",
            max_pending_requests=10,
        ),
        map_batches_kwargs=dict(
            zero_copy_batch=True,
            concurrency=1,
            max_concurrency=4,
            accelerator_type="L4",
        ),
    )

    assert stage.fn_constructor_kwargs == {
        "model": "facebook/opt-125m",
        "task_type": "generate",
        "max_pending_requests": 10,
        "engine_kwargs": {
            "tensor_parallel_size": 4,
            "pipeline_parallel_size": 2,
            "distributed_executor_backend": "ray",
        },
    }
    scheduling_strategy = stage.map_batches_kwargs.pop("scheduling_strategy")
    assert stage.map_batches_kwargs == {
        "zero_copy_batch": True,
        "concurrency": 1,
        "max_concurrency": 4,
        "accelerator_type": "L4",
        "num_gpus": 0,
        "runtime_env": {},
    }
    breakpoint()
    assert isinstance(scheduling_strategy, PlacementGroupSchedulingStrategy)
