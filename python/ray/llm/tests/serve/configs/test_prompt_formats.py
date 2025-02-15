import pytest

from ray.llm._internal.serve.configs.prompt_formats import (
    HTTPException,
    HuggingFacePromptFormat,
    VisionPromptFormat,
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


#########################
# Vision Prompt Format #
#########################

text = "Describe the image."
url = "https://www.image.jpg"

message_data = {
    "role": "user",
    "content": [
        {
            "type": "text",
            "text": text,
        },
        {
            "type": "image_url",
            "image_url": {
                "url": url,
            },
        },
    ],
}

message = Message(**message_data)


@pytest.fixture
def vision_prompt_format():
    """Fixture for VisionPromptFormat."""
    prompt_format_data = {
        "system": "{instruction}",
        "assistant": "{instruction}",
        "trailing_assistant": "",
        "user": "<image>\nUSER: {instruction}\nASSISTANT:",
        "vision": True,
    }
    return VisionPromptFormat(**prompt_format_data)


def test_text_content():
    data = {
        "type": "text",
        "wrong_field": "foo",
    }
    with pytest.raises(ValueError):
        TextContent(**data)


@pytest.mark.parametrize(
    "image_content_data",
    [
        {
            "type": "image_url",
            "wrong_field": {
                "url": "https://www.image.jpg",
            },
        },
        {
            "type": "image_url",
            "image_url": {
                "another_url": "https://www.image.jpg",
            },
        },
    ],
)
def test_image_content(image_content_data):
    with pytest.raises(ValueError):
        ImageContent(**message_data)


@pytest.mark.parametrize("messages", [[message], Prompt(prompt=[message])])
def test_vision_prompt_format(messages, vision_prompt_format):
    prompt_output = vision_prompt_format.generate_prompt(messages)
    assert prompt_output.text == "<image>\nUSER: Describe the image.\nASSISTANT:"
    assert len(prompt_output.image) == 1
    assert prompt_output.image[0].image_url == url


@pytest.mark.parametrize(
    "messages",
    [
        "This is a test message.",
        Prompt(
            prompt=[
                Message(
                    **{
                        "role": "user",
                        "content": [
                            {"type": "text", "text": "This is a test message."}
                        ],
                    }
                )
            ]
        ),
    ],
)
def test_vision_prompt_format_no_image(messages, vision_prompt_format):
    prompt_output = vision_prompt_format.generate_prompt(messages)
    assert prompt_output.text == "This is a test message."
    assert prompt_output.image is None


def test_vision_prompt_format_multiple_image(vision_prompt_format):
    message_with_two_image = {
        "role": "user",
        "content": [
            {
                "type": "text",
                "text": "baz",
            },
            {
                "type": "image_url",
                "image_url": {
                    "url": "foo",
                },
            },
            {
                "type": "image_url",
                "image_url": {
                    "url": "bar",
                },
            },
        ],
    }
    with pytest.raises(HTTPException) as e:
        vision_prompt_format.generate_prompt(
            Prompt(prompt=[Message(**message_with_two_image)])
        )

    assert e.value.status_code == 400
    assert "The message contains 2 image contents." in str(e)


def test_vision_prompt_format_wrong_role(vision_prompt_format):
    message_with_wrong_role = {"role": "system", "content": "foo"}
    with pytest.raises(HTTPException) as e:
        vision_prompt_format.generate_prompt(
            Prompt(prompt=[Message(**message_with_wrong_role)])
        )

    assert e.value.status_code == 400
    assert "The message has a role of system." in str(e)


def test_vision_prompt_format_multiple_message(vision_prompt_format):
    msg1 = {
        "role": "user",
        "content": [
            {
                "type": "text",
                "text": "baz",
            },
            {
                "type": "image_url",
                "image_url": {
                    "url": "foo",
                },
            },
        ],
    }
    with pytest.raises(HTTPException) as e:
        vision_prompt_format.generate_prompt(
            Prompt(prompt=[Message(**msg1), Message(**msg1)])
        )

    assert e.value.status_code == 400
    assert "There are 2 messages." in str(e)


def test_vision_prompt_format_content_none(vision_prompt_format):
    msg = {"role": "user", "content": None}
    with pytest.raises(ValueError):
        vision_prompt_format.generate_prompt(Prompt(prompt=[Message(**msg)]))


def test_vision_prompt_format_multiple_text_content(vision_prompt_format):
    msg = {
        "role": "user",
        "content": [
            {
                "type": "text",
                "text": "baz",
            },
            {
                "type": "text",
                "text": "bar",
            },
            {
                "type": "image_url",
                "image_url": {
                    "url": "foo",
                },
            },
        ],
    }
    with pytest.raises(HTTPException) as e:
        vision_prompt_format.generate_prompt(Prompt(prompt=[Message(**msg)]))

    assert e.value.status_code == 400
    assert "The message contains more than one text type content." in str(e)


def test_vision_prompt_format_no_text_content(vision_prompt_format):
    msg = {
        "role": "user",
        "content": [
            {
                "type": "image_url",
                "image_url": {
                    "url": "foo",
                },
            },
        ],
    }
    with pytest.raises(HTTPException) as e:
        vision_prompt_format.generate_prompt(Prompt(prompt=[Message(**msg)]))

    assert e.value.status_code == 400
    assert "The message contains zero text type content." in str(e)


###############
# Common Test #
###############


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
