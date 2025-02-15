import pytest

from ray.llm._internal.serve.configs.prompt_formats import (
    HuggingFacePromptFormat,
    ContentContent,
    ImageContent,
    TextContent,
    Message,
    Prompt,
)


@pytest.fixture
def hf_prompt_format(download_model_ckpt):
    hf_prompt_format = HuggingFacePromptFormat(use_hugging_face_chat_template=True)
    hf_prompt_format.set_processor(model_id_or_path=download_model_ckpt)
    return hf_prompt_format


def test_hf_prompt_format_on_string_message(hf_prompt_format):
    messages = Prompt(prompt="This is a test message.")
    with pytest.raises(ValueError):
        hf_prompt_format.generate_prompt(messages=messages)


def test_hf_prompt_format_on_prompt_object(hf_prompt_format):
    messages = Prompt(
        prompt=[
            Message(role="system", content="You are a helpful assistant."),
            Message(
                role="user",
                content=[
                    ContentContent(field="text", content="Can this animal"),
                    ImageContent(
                        field="image_url",
                        image_url={"url": "https://example.com/dog.jpg"},
                    ),
                    ContentContent(field="text", content="live here?"),
                    ImageContent(
                        field="image_url",
                        image_url={"url": "https://example.com/mountain.jpg"},
                    ),
                ],
            ),
            Message(
                role="assistant",
                content="It looks like you've shared an image of a "
                "dog lying on a wooden floor, and another "
                "image depicting a serene landscape with a "
                "sunset over a snowy hill or mountain.",
            ),
            Message(
                role="user",
                content="So you are suggesting you can find a poppy living in the snowy mountain?",
            ),
        ],
    )

    formated_prompt = hf_prompt_format.generate_prompt(messages=messages)
    assert formated_prompt.text == (
        "<s>[INST]Can this animal[IMG]live here?[IMG][/INST]It looks like you've "
        "shared an image of a dog lying on a wooden floor, and another image "
        "depicting a serene landscape with a sunset over a snowy hill or "
        "mountain.</s>[INST]You are a helpful assistant.\n\nSo you are suggesting "
        "you can find a poppy living in the snowy mountain?[/INST]"
    )
    assert len(formated_prompt.image) == 2
    assert formated_prompt.image[0].image_url == "https://example.com/dog.jpg"
    assert formated_prompt.image[1].image_url == "https://example.com/mountain.jpg"


def test_hf_prompt_format_on_list_of_messages(hf_prompt_format):
    messages = [
        Message(role="system", content="You are a helpful assistant."),
        Message(
            role="user",
            content=[
                ContentContent(field="text", content="Can this animal"),
                ImageContent(
                    field="image_url",
                    image_url={"url": "https://example.com/dog.jpg"},
                ),
                ContentContent(field="text", content="live here?"),
                ImageContent(
                    field="image_url",
                    image_url={"url": "https://example.com/mountain.jpg"},
                ),
            ],
        ),
        Message(
            role="assistant",
            content="It looks like you've shared an image of a "
            "dog lying on a wooden floor, and another "
            "image depicting a serene landscape with a "
            "sunset over a snowy hill or mountain.",
        ),
        Message(
            role="user",
            content="So you are suggesting you can find a poppy living in the snowy mountain?",
        ),
    ]

    formated_prompt = hf_prompt_format.generate_prompt(messages=messages)
    assert len(formated_prompt.image) == 2
    assert formated_prompt.text == (
        "<s>[INST]Can this animal[IMG]live here?[IMG][/INST]It looks like you've "
        "shared an image of a dog lying on a wooden floor, and another image "
        "depicting a serene landscape with a sunset over a snowy hill or "
        "mountain.</s>[INST]You are a helpful assistant.\n\nSo you are suggesting "
        "you can find a poppy living in the snowy mountain?[/INST]"
    )
    assert formated_prompt.image[0].image_url == "https://example.com/dog.jpg"
    assert formated_prompt.image[1].image_url == "https://example.com/mountain.jpg"


def test_validation_message():
    # check that message with assistant role can have content that
    # is a string or none, but nothing else
    Message.model_validate({"role": "assistant", "content": "Hello, World!"})

    Message.model_validate({"role": "assistant", "content": ""})

    Message.model_validate({"role": "assistant", "content": None})

    with pytest.raises(ValueError):
        Message.model_validate(
            {
                "role": "assistant",
                "content": {
                    "NOT_VALID",
                },
            }
        )

    # Test system and user roles
    for role in ["system", "user"]:
        # this should pass
        Message.model_validate({"role": role, "content": "Hello, World!"})

        Message.model_validate({"role": role, "content": ""})

        # a non string content should raise an error

        with pytest.raises(ValueError):
            Message.model_validate(
                {
                    "role": role,
                    "content": {
                        "NOT_VALID",
                    },
                }
            )

        with pytest.raises(ValueError):
            Message.model_validate({"role": role, "content": None})

    # test message with image.
    Message(
        role="user",
        content=[
            TextContent(type="text", text="This is a test."),
            ImageContent(type="image_url", image_url={"url": "foo"}),
        ],
    )
