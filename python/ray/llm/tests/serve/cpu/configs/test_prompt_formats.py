import sys

import pytest
from pydantic import ValidationError

from ray.llm._internal.serve.configs.prompt_formats import (
    Image,
    Message,
    Prompt,
    Text,
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
            Text(type="text", text="This is a test."),
            Image(type="image_url", image_url={"url": "foo"}),
        ],
    )


def test_prompt_validation():
    # Test valid prompt creation
    Prompt(prompt="This is a test message.")

    Prompt(
        prompt=[
            Message(role="system", content="You are a helpful assistant."),
            Message(role="user", content="Hello!"),
        ]
    )

    # Test invalid prompt creation
    with pytest.raises(ValidationError):
        # Empty list should raise error
        Prompt(prompt=[])


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
