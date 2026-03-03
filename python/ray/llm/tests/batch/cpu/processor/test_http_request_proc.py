import sys

import pytest

from ray.data.llm import HttpRequestStageConfig
from ray.llm._internal.batch.processor import ProcessorBuilder
from ray.llm._internal.batch.processor.http_request_proc import (
    HttpRequestProcessorConfig,
)


def test_http_request_processor():
    config = HttpRequestProcessorConfig(
        url="http://localhost:8000",
        headers={"Authorization": "Bearer 1234567890"},
        qps=2,
        concurrency=4,
        batch_size=64,
        http_request_stage=HttpRequestStageConfig(
            num_cpus=0.5,
            memory=100000,
        ),
    )
    processor = ProcessorBuilder.build(config)
    assert processor.list_stage_names() == ["HttpRequestStage"]
    stage = processor.get_stage_by_name("HttpRequestStage")
    assert stage.map_batches_kwargs["num_cpus"] == 0.5
    assert stage.map_batches_kwargs["memory"] == 100000
    assert stage.map_batches_kwargs["compute"].min_size == 1
    assert stage.map_batches_kwargs["compute"].max_size == 4
    assert stage.fn_constructor_kwargs["url"] == "http://localhost:8000"
    assert stage.fn_constructor_kwargs["additional_header"] == {
        "Authorization": "Bearer 1234567890"
    }
    assert stage.fn_constructor_kwargs["qps"] == 2


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
