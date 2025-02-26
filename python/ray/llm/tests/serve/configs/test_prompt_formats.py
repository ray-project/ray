import pytest
import sys

from ray.llm._internal.serve.configs.prompt_formats import (
    HuggingFacePromptFormat,
    Content,
    Image,
    Text,
    Message,
    Prompt,
)

from pydantic import ValidationError


@pytest.fixture
def hf_prompt_format(model_pixtral_12b):
    hf_prompt_format = HuggingFacePromptFormat()
    hf_prompt_format.set_processor(model_id_or_path=model_pixtral_12b)
    return hf_prompt_format


def test_hf_prompt_format_on_string_message(hf_prompt_format):
    messages = Prompt(prompt="This is a test message.")
    with pytest.raises(ValueError):
        hf_prompt_format.generate_prompt(messages=messages)


def test_hf_prompt_format_on_prompt_object(hf_prompt_format):
    # Test if generate_prompt() can handle messages structured as a Prompt object.
    messages = Prompt(
        prompt=[
            Message(role="system", content="You are a helpful assistant."),
            Message(
                role="user",
                content=[
                    Content(field="text", content="Can this animal"),
                    Image(
                        field="image_url",
                        image_url={"url": "https://example.com/dog.jpg"},
                    ),
                    Content(field="text", content="live here?"),
                    Image(
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


def test_hf_prompt_format_on_prompt_dict(hf_prompt_format):
    """Test if generate_prompt() can handle a Prompt object structured as a dictionary."""
    messages = {
        "prompt": [
            {"role": "system", "content": "You are a helpful assistant."},
            {
                "role": "user",
                "content": [
                    {"field": "text", "content": "Can this animal"},
                    {
                        "field": "image_url",
                        "image_url": {"url": "https://example.com/dog.jpg"},
                    },
                    {"field": "text", "content": "live here?"},
                    {
                        "field": "image_url",
                        "image_url": {"url": "https://example.com/mountain.jpg"},
                    },
                ],
            },
            {
                "role": "assistant",
                "content": (
                    "It looks like you've shared an image of a "
                    "dog lying on a wooden floor, and another "
                    "image depicting a serene landscape with a "
                    "sunset over a snowy hill or mountain."
                ),
            },
            {
                "role": "user",
                "content": "So you are suggesting you can find a poppy living in the snowy mountain?",
            },
        ],
    }
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
    """Test if generate_prompt() can handle a list of Message objects."""
    messages = [
        Message(role="system", content="You are a helpful assistant."),
        Message(
            role="user",
            content=[
                Content(field="text", content="Can this animal"),
                Image(
                    field="image_url",
                    image_url={"url": "https://example.com/dog.jpg"},
                ),
                Content(field="text", content="live here?"),
                Image(
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


def test_hf_prompt_format_on_list_of_messages_dict(hf_prompt_format):
    """Test if generate_prompt() can handle a list of Message objects structured as dictionaries."""

    messages = [
        {"role": "system", "content": "You are a helpful assistant."},
        {
            "role": "user",
            "content": [
                {"field": "text", "content": "Can this animal"},
                {
                    "field": "image_url",
                    "image_url": {"url": "https://example.com/dog.jpg"},
                },
                {"field": "text", "content": "live here?"},
                {
                    "field": "image_url",
                    "image_url": {"url": "https://example.com/mountain.jpg"},
                },
            ],
        },
        {
            "role": "assistant",
            "content": (
                "It looks like you've shared an image of a "
                "dog lying on a wooden floor, and another "
                "image depicting a serene landscape with a "
                "sunset over a snowy hill or mountain."
            ),
        },
        {
            "role": "user",
            "content": "So you are suggesting you can find a poppy living in the snowy mountain?",
        },
    ]
    formatted_prompt = hf_prompt_format.generate_prompt(messages=messages)

    assert formatted_prompt.text == (
        "<s>[INST]Can this animal[IMG]live here?[IMG][/INST]It looks like you've "
        "shared an image of a dog lying on a wooden floor, and another image "
        "depicting a serene landscape with a sunset over a snowy hill or "
        "mountain.</s>[INST]You are a helpful assistant.\n\nSo you are suggesting "
        "you can find a poppy living in the snowy mountain?[/INST]"
    )
    assert len(formatted_prompt.image) == 2
    assert formatted_prompt.image[0].image_url == "https://example.com/dog.jpg"
    assert formatted_prompt.image[1].image_url == "https://example.com/mountain.jpg"


def test_invalid_hf_prompt_formats(hf_prompt_format):
    """Test invalid formats for generate_prompt() to ensure validation errors are raised."""

    # Invalid at initialization:
    with pytest.raises(ValidationError):
        # Prompt is not a list
        Prompt(prompt=Message(role="system", content="You are a helpful assistant.")),

    with pytest.raises(ValidationError):
        # Content is None for a "user" role
        Prompt(prompt=[Message(role="user", content=None)]),

    with pytest.raises(ValidationError):
        # Message with an invalid role
        Prompt(prompt=[Message(role="invalid_role", content="Invalid role")]),

    # Invalid at generate_prompt():
    invalid_messages = [
        # Empty list
        [],
        # List of Messages mixed with invalid strings
        ["string_instead_of_message", Message(role="user", content="Valid message")],
        # Prompt as a single dict instead of list of Message dicts
        {"prompt": {"role": "system", "content": "You are a helpful assistant."}},
        # Empty prompt list
        {"prompt": []},
        # Invalid role in the message
        {"prompt": [{"role": "invalid_role", "content": "Invalid role"}]},
        # Mixed list containing dicts and Message objects
        [
            {"role": "system", "content": "You are a helpful assistant."},
            Message(role="user", content="Valid message"),
        ],
        # List of invalid message dict
        [{"role": "system", "invalid_key": "Invalid structure"}],
    ]
    # Test all invalid cases
    for invalid_message in invalid_messages:
        with pytest.raises((ValidationError, ValueError)):
            hf_prompt_format.generate_prompt(messages=invalid_message)


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


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
