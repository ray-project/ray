import pytest

from ray.llm._internal.serve.configs.prompt_formats import (
    ImageContent,
    Message,
    TextContent,
)


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
