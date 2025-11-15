import sys

import pytest

from ray.data.llm import MultimodalProcessorConfig, build_llm_processor


@pytest.mark.parametrize("chat_template_content_format", ["string", "openai"])
def test_multi_modal_processor(chat_template_content_format):
    multimodal_processor_config_kwargs = dict(
        model="Qwen/Qwen2.5-VL-3B-Instruct",
        concurrency=4,
        batch_size=64,
    )

    if chat_template_content_format == "openai":
        multimodal_processor_config_kwargs.update(dict(
            chat_template_content_format=chat_template_content_format,
        ))

    config = MultimodalProcessorConfig(**multimodal_processor_config_kwargs)
    processor = build_llm_processor(config)
    assert processor.list_stage_names() == ["PrepareMultimodalStage"]
    stage = processor.get_stage_by_name("PrepareMultimodalStage")
    assert stage.fn_constructor_kwargs == {
        "model": "Qwen/Qwen2.5-VL-3B-Instruct",
        "chat_template_content_format": chat_template_content_format,
    }
    assert stage.map_batches_kwargs == {
        "zero_copy_batch": True,
        "concurrency": 4,
        "batch_size": 64,
    }


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
