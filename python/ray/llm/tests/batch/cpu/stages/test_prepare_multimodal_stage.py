import sys

import pytest

from ray.llm._internal.batch.stages.prepare_multimodal_stage import (
    PrepareMultimodalUDF,
)


@pytest.mark.asyncio
async def test_prepare_multimodal_udf_image_url():
    udf = PrepareMultimodalUDF(
        data_column="__data",
        expected_input_keys=["messages"],
        model="Qwen/Qwen2.5-VL-3B-Instruct",
        chat_template_content_format="string",
    )

    batch = {
        "__data": [
            {
                "messages": [
                    {"role": "system", "content": "You are a helpful assistant."},
                    {
                        "role": "user",
                        "content": [
                            {
                                "type": "text",
                                "text": "Describe this image in 10 words.",
                            },
                            {
                                "type": "image_url",
                                "image_url": {
                                    "url": "https://vllm-public-assets.s3.us-west-2.amazonaws.com/vision_model_images/cherry_blossom.jpg"
                                },
                                "uuid": "image-1-id",
                            },
                            {
                                "type": "image_url",
                                "image_url": {
                                    "url": "https://vllm-public-assets.s3.us-west-2.amazonaws.com/vision_model_images/cherry_blossom.jpg"
                                },
                                "uuid": "image-2-id",
                            },
                        ],
                    },
                ]
            }
        ]
    }

    results = []
    async for result in udf(batch):
        results.append(result["__data"][0])

    assert len(results) == 1
    assert "multimodal_data" in results[0]
    assert len(results[0]["multimodal_data"]["image"]) == 2
    assert "multimodal_uuids" in results[0]
    assert results[0]["multimodal_uuids"] == {"image": ["image-1-id", "image-2-id"]}
    assert "messages" in results[0]


@pytest.mark.asyncio
async def test_prepare_multimodal_udf_pil_image(image_asset):
    _, image_pil = image_asset

    udf = PrepareMultimodalUDF(
        data_column="__data",
        expected_input_keys=["messages"],
        model="Qwen/Qwen2.5-VL-3B-Instruct",
        chat_template_content_format="string",
    )

    batch = {
        "__data": [
            {
                "messages": [
                    {"role": "system", "content": "You are a helpful assistant."},
                    {
                        "role": "user",
                        "content": [
                            {
                                "type": "text",
                                "text": "Describe this image in 10 words.",
                            },
                            {
                                "type": "image_pil",
                                "image_pil": image_pil,
                            },
                        ],
                    },
                ]
            }
        ]
    }

    results = []
    async for result in udf(batch):
        results.append(result["__data"][0])

    assert len(results) == 1
    assert "multimodal_data" in results[0]
    assert "messages" in results[0]


@pytest.mark.asyncio
async def test_prepare_multimodal_udf_no_multimodal_content():
    """
    Multimodal stage should proceed as normal if there is no multimodal content provided in messages.
    """
    udf = PrepareMultimodalUDF(
        data_column="__data",
        expected_input_keys=["messages"],
        model="Qwen/Qwen2.5-VL-3B-Instruct",
        chat_template_content_format="string",
    )

    batch = {
        "__data": [
            {
                "messages": [
                    {"role": "system", "content": "You are a helpful assistant."},
                    {"role": "user", "content": "Hello, how are you?"},
                ]
            }
        ]
    }

    results = []
    async for result in udf(batch):
        results.append(result["__data"][0])

    assert len(results) == 1
    assert "multimodal_data" in results[0]
    assert results[0]["multimodal_data"] is None
    assert "messages" in results[0]


def test_prepare_multimodal_udf_expected_keys():
    udf = PrepareMultimodalUDF(
        data_column="__data",
        expected_input_keys=["messages"],
        model="Qwen/Qwen2.5-VL-3B-Instruct",
        chat_template_content_format="string",
    )
    assert udf.expected_input_keys == {"messages"}


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
