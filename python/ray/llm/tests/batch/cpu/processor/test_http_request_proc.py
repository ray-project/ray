import sys

import pytest

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
    )
    processor = ProcessorBuilder.build(config)
    assert processor.list_stage_names() == ["HttpRequestStage"]
    stage = processor.get_stage_by_name("HttpRequestStage")
    assert stage.map_batches_kwargs["concurrency"] == 4
    assert stage.fn_constructor_kwargs["url"] == "http://localhost:8000"
    assert stage.fn_constructor_kwargs["additional_header"] == {
        "Authorization": "Bearer 1234567890"
    }
    assert stage.fn_constructor_kwargs["qps"] == 2


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
