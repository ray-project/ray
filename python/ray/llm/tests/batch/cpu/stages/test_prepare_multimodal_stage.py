import io
import sys

import PIL.Image
import pytest
import requests

from ray.llm._internal.batch.stages.prepare_multimodal_stage import (
    PrepareMultimodalUDF,
)


@pytest.mark.asyncio
async def test_prepare_multimodal_udf_image_url():
    udf = PrepareMultimodalUDF(
        data_column="__data",
        expected_input_keys=["messages"],
        model="Qwen/Qwen2.5-VL-3B-Instruct"
    )

    batch = {
        "__data": [
            {
                "messages": [
                    {"role": "system", "content": "You are a helpful assistant."},
                    {
                        "role": "user",
                        "content": [
                            {"type": "text", "text": "Describe this image in 10 words."},
                            {
                                "type": "image_url",
                                "image_url": {"url": "https://vllm-public-assets.s3.us-west-2.amazonaws.com/vision_model_images/cherry_blossom.jpg"}
                            }
                        ]
                    }
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
async def test_prepare_multimodal_udf_multiple_image_urls():
    udf = PrepareMultimodalUDF(
        data_column="__data",
        expected_input_keys=["messages"],
        model="Qwen/Qwen2.5-VL-3B-Instruct"
    )

    batch = {
        "__data": [
            {
                "messages": [
                    {"role": "system", "content": "You are a helpful assistant."},
                    {
                        "role": "user",
                        "content": [
                            {"type": "text", "text": "Describe these images in 10 words."},
                            {
                                "type": "image_url",
                                "image_url": {"url": "https://vllm-public-assets.s3.us-west-2.amazonaws.com/vision_model_images/cherry_blossom.jpg"}
                            },
                            {
                                "type": "image_url",
                                "image_url": {"url": "https://vllm-public-assets.s3.us-west-2.amazonaws.com/vision_model_images/cherry_blossom.jpg"}
                            }
                        ]
                    }
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
    assert "messages" in results[0]


@pytest.mark.asyncio
async def test_prepare_multimodal_udf_pil_image():
    image_url = "https://vllm-public-assets.s3.us-west-2.amazonaws.com/vision_model_images/cherry_blossom.jpg"
    response = requests.get(image_url)
    image_pil = PIL.Image.open(io.BytesIO(response.content))

    udf = PrepareMultimodalUDF(
        data_column="__data",
        expected_input_keys=["messages"],
        model="Qwen/Qwen2.5-VL-3B-Instruct"
    )

    batch = {
        "__data": [
            {
                "messages": [
                    {"role": "system", "content": "You are a helpful assistant."},
                    {
                        "role": "user",
                        "content": [
                            {"type": "text", "text": "Describe this image in 10 words."},
                            {
                                "type": "image_pil",
                                "image_pil": image_pil,
                            }
                        ]
                    }
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
    udf = PrepareMultimodalUDF(
        data_column="__data",
        expected_input_keys=["messages"],
        model="Qwen/Qwen2.5-VL-3B-Instruct"
    )

    batch = {
        "__data": [
            {
                "messages": [
                    {"role": "system", "content": "You are a helpful assistant."},
                    {"role": "user", "content": "Hello, how are you?"}
                ]
            }
        ]
    }

    results = []
    async for result in udf(batch):
        results.append(result["__data"][0])

    assert len(results) == 1
    assert "multimodal_data" in results[0]
    assert results[0]["multimodal_data"] == {}
    assert "messages" in results[0]


def test_prepare_multimodal_udf_expected_keys():
    udf = PrepareMultimodalUDF(
        data_column="__data",
        expected_input_keys=["messages"],
        model="Qwen/Qwen2.5-VL-3B-Instruct"
    )
    assert udf.expected_input_keys == {"messages"}


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
