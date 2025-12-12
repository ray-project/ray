import sys
from typing import Any, Dict

import pytest

import ray
from ray import serve
from ray.data.llm import ServeDeploymentProcessorConfig, build_processor
from ray.llm._internal.batch.processor import ProcessorBuilder
from ray.serve.llm.openai_api_models import ChatCompletionRequest, CompletionRequest


@pytest.mark.parametrize(
    "dtype_mapping", [None, {"CompletionRequest": CompletionRequest}]
)
def test_serve_deployment_processor(dtype_mapping):
    app_name = "test_serve_deployment_processor_app"
    deployment_name = "test_serve_deployment_name"

    config_kwargs = dict(
        deployment_name=deployment_name,
        app_name=app_name,
        batch_size=16,
        concurrency=1,
    )
    if dtype_mapping is not None:
        config_kwargs["dtype_mapping"] = dtype_mapping
    config = ServeDeploymentProcessorConfig(**config_kwargs)

    processor = ProcessorBuilder.build(config)
    assert processor.list_stage_names() == [
        "ServeDeploymentStage",
    ]

    stage = processor.get_stage_by_name("ServeDeploymentStage")
    assert stage.fn_constructor_kwargs == {
        "deployment_name": deployment_name,
        "app_name": app_name,
        "dtype_mapping": dtype_mapping,
        "should_continue_on_error": False,
    }

    assert stage.map_batches_kwargs == {
        "concurrency": 1,
    }


def test_simple_serve_deployment(serve_cleanup):
    @serve.deployment
    class SimpleServeDeployment:
        # ServeDeploymentStageUDF expects an async generator.
        async def add(self, request: Dict[str, Any]):
            yield {"result": request["x"] + 1}

    app_name = "simple_serve_deployment_app"
    deployment_name = "SimpleServeDeployment"

    serve.run(SimpleServeDeployment.bind(), name=app_name)

    config = ServeDeploymentProcessorConfig(
        deployment_name=deployment_name,
        app_name=app_name,
        batch_size=16,
        concurrency=1,
    )

    processor = build_processor(
        config,
        preprocess=lambda row: dict(
            method="add",
            dtype=None,  # Empty dtype since output is already dict format
            request_kwargs=dict(x=row["id"]),
        ),
        postprocess=lambda row: dict(
            resp=row["result"],
            id=row["id"],
        ),
    )

    ds = ray.data.range(60)
    ds = ds.map(lambda x: {"id": x["id"]})
    ds = processor(ds)

    outs = ds.take_all()
    assert len(outs) == 60
    assert all("resp" in out for out in outs)
    assert all(out["resp"] == out["id"] + 1 for out in outs)


def test_serve_deployment_continue_on_error(serve_cleanup):
    """Integration test: pipeline continues when some rows fail with continue_on_error."""

    @serve.deployment
    class FailingServeDeployment:
        async def process(self, request: Dict[str, Any]):
            x = request["x"]
            if x % 10 == 0:  # Fail every 10th row
                raise ValueError(f"Intentional failure for x={x}")
            yield {"result": x * 2}

    app_name = "failing_serve_deployment_app"
    deployment_name = "FailingServeDeployment"

    serve.run(FailingServeDeployment.bind(), name=app_name)

    config = ServeDeploymentProcessorConfig(
        deployment_name=deployment_name,
        app_name=app_name,
        batch_size=16,
        concurrency=1,
        should_continue_on_error=True,
    )

    processor = build_processor(
        config,
        preprocess=lambda row: dict(
            method="process",
            dtype=None,
            request_kwargs=dict(x=row["id"]),
        ),
        postprocess=lambda row: dict(
            resp=row.get("result"),
            error=row.get("__inference_error__"),
        ),
    )

    ds = ray.data.range(60)
    ds = ds.map(lambda x: {"id": x["id"]})
    ds = processor(ds)

    outs = ds.take_all()
    assert len(outs) == 60  # All rows processed, none dropped

    errors = [o for o in outs if o["error"] is not None]
    successes = [o for o in outs if o["error"] is None]

    # Rows 0, 10, 20, 30, 40, 50 should fail (6 total)
    assert len(errors) == 6
    assert len(successes) == 54

    # Verify error messages contain expected info
    for e in errors:
        assert "ValueError" in e["error"]
        assert "Intentional failure" in e["error"]

    # Verify successful rows have correct result
    for s in successes:
        assert s["resp"] is not None


def test_completion_model(model_opt_125m, create_model_opt_125m_deployment):
    deployment_name, app_name = create_model_opt_125m_deployment
    config = ServeDeploymentProcessorConfig(
        deployment_name=deployment_name,
        app_name=app_name,
        dtype_mapping={
            "CompletionRequest": CompletionRequest,
        },
        batch_size=16,
        concurrency=1,
    )

    processor = build_processor(
        config,
        preprocess=lambda row: dict(
            method="completions",
            dtype="CompletionRequest",
            request_kwargs=dict(
                model=model_opt_125m,
                prompt=row["prompt"],
                stream=False,
            ),
        ),
        postprocess=lambda row: dict(
            resp=row["choices"][0]["text"],
        ),
    )

    ds = ray.data.range(60)
    ds = ds.map(lambda x: {"prompt": f"Hello {x['id']}"})
    ds = processor(ds)
    ds = ds.materialize()
    outs = ds.take_all()
    assert len(outs) == 60
    assert all("resp" in out for out in outs)


def test_multi_turn_completion_model(model_opt_125m, create_model_opt_125m_deployment):
    deployment_name, app_name = create_model_opt_125m_deployment

    config1 = ServeDeploymentProcessorConfig(
        deployment_name=deployment_name,
        app_name=app_name,
        dtype_mapping={
            "CompletionRequest": CompletionRequest,
        },
        # Use lower batch size to reduce resource usage as there are multiple processors
        batch_size=4,
        concurrency=1,
    )

    processor1 = build_processor(
        config1,
        preprocess=lambda row: dict(
            dtype="CompletionRequest",
            method="completions",
            request_kwargs=dict(
                model=model_opt_125m,
                prompt=row["prompt"],
                stream=False,
            ),
        ),
        postprocess=lambda row: dict(
            prompt=row["choices"][0]["text"],
        ),
    )

    config2 = ServeDeploymentProcessorConfig(
        deployment_name=deployment_name,
        app_name=app_name,
        dtype_mapping={
            "CompletionRequest": CompletionRequest,
        },
        batch_size=4,
        concurrency=1,
    )

    processor2 = build_processor(
        config2,
        preprocess=lambda row: dict(
            dtype="CompletionRequest",
            method="completions",
            request_kwargs=dict(
                model=model_opt_125m,
                prompt=row["prompt"],
                stream=False,
            ),
        ),
        postprocess=lambda row: dict(
            resp=row["choices"][0]["text"],
        ),
    )

    ds = ray.data.range(60)
    ds = ds.map(lambda x: {"prompt": f"Hello {x['id']}"})
    ds = processor1(ds)
    ds = processor2(ds)

    ds = ds.materialize()
    outs = ds.take_all()
    assert len(outs) == 60
    assert all("resp" in out for out in outs)


def test_chat_model(model_opt_125m, create_model_opt_125m_deployment):
    deployment_name, app_name = create_model_opt_125m_deployment
    config = ServeDeploymentProcessorConfig(
        deployment_name=deployment_name,
        app_name=app_name,
        dtype_mapping={
            "ChatCompletionRequest": ChatCompletionRequest,
        },
        batch_size=16,
        concurrency=1,
    )

    processor = build_processor(
        config,
        preprocess=lambda row: dict(
            dtype="ChatCompletionRequest",
            method="chat",
            request_kwargs=dict(
                model=model_opt_125m,
                messages=[
                    {"role": "system", "content": "You are a helpful assistant"},
                    {"role": "user", "content": f"Hello {row['id']}"},
                ],
                stream=False,
            ),
        ),
        postprocess=lambda row: dict(
            resp=row["choices"][0]["message"]["content"],
        ),
    )

    ds = ray.data.range(60)
    ds = ds.map(lambda x: {"id": x["id"]})
    ds = processor(ds)
    ds = ds.materialize()
    outs = ds.take_all()
    assert len(outs) == 60
    assert all("resp" in out for out in outs)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
