import sys
from typing import Any, Dict

import pytest

import ray
from ray import serve
from ray.llm._internal.batch.processor import ProcessorBuilder
from ray.llm._internal.batch.processor.serve_deployment_proc import (
    ServeDeploymentProcessorConfig,
)


def test_serve_deployment_processor():
    app_name = "test_serve_deployment_processor_app"
    deployment_name = "test_serve_deployment_name"

    config = ServeDeploymentProcessorConfig(
        deployment_name=deployment_name,
        app_name=app_name,
        batch_size=16,
        concurrency=1,
    )

    processor = ProcessorBuilder.build(config)
    assert processor.list_stage_names() == [
        "ServeDeploymentStage",
    ]

    stage = processor.get_stage_by_name("ServeDeploymentStage")
    assert stage.fn_constructor_kwargs == {
        "deployment_name": deployment_name,
        "app_name": app_name,
    }

    assert stage.map_batches_kwargs == {
        "concurrency": 1,
    }


def test_simple_serve_deployment():
    @serve.deployment
    class SimpleServeDeployment:
        # ServeDeploymentStageUDF expects a generator
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

    processor = ProcessorBuilder.build(
        config,
        preprocess=lambda row: dict(
            method="add",
            dtype=None,  # Empty dtype since output is already dict format
            request_kwargs=dict(x=row["id"]),
        ),
        postprocess=lambda row: dict(resp=row["result"]),
    )

    ds = ray.data.range(60)
    ds = ds.map(lambda x: {"id": x["id"]})
    ds = processor(ds)

    outs = ds.take_all()
    assert len(outs) == 60
    assert all("resp" in out for out in outs)

    for i, out in enumerate(outs):
        assert out["resp"] == i + 1

    serve.shutdown()


def test_completion_model(model_opt_125m, create_model_opt_125m_deployment):
    deployment_name, app_name = create_model_opt_125m_deployment
    config = ServeDeploymentProcessorConfig(
        deployment_name=deployment_name,
        app_name=app_name,
        batch_size=16,
        concurrency=1,
    )

    processor = ProcessorBuilder.build(
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
        # Use lower batch size to reduce resource usage as there are multiple processors
        batch_size=4,
        concurrency=1,
    )

    processor1 = ProcessorBuilder.build(
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
        batch_size=4,
        concurrency=1,
    )

    processor2 = ProcessorBuilder.build(
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
        batch_size=16,
        concurrency=1,
    )

    processor = ProcessorBuilder.build(
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
