import pytest

from ray.llm._internal.serve.configs.rayllm_models import (
    HTTPException,
    ImageContent,
    Message,
    Prompt,
    TextContent,
    VisionPromptFormat,
)

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

prompt_format_data = {
    "system": "{instruction}",  # Not used for now
    "assistant": "{instruction}",  # not used for now
    "trailing_assistant": "",
    "user": "<image>\nUSER: {instruction}\nASSISTANT:",
    "vision": True,
}

prompt_format = VisionPromptFormat(**prompt_format_data)


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
def test_prompt_format(messages):
    prompt_output = prompt_format.generate_prompt(messages)
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
def test_prompt_format_no_image(messages):
    prompt_output = prompt_format.generate_prompt(messages)
    assert prompt_output.text == "This is a test message."
    assert prompt_output.image is None


def test_prompt_format_multiple_image():
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
        prompt_format.generate_prompt(
            Prompt(prompt=[Message(**message_with_two_image)])
        )

    assert e.value.status_code == 400
    assert "The message contains 2 image contents." in str(e)


def test_prompt_format_wrong_role():
    message_with_wrong_role = {"role": "system", "content": "foo"}
    with pytest.raises(HTTPException) as e:
        prompt_format.generate_prompt(
            Prompt(prompt=[Message(**message_with_wrong_role)])
        )

    assert e.value.status_code == 400
    assert "The message has a role of system." in str(e)


def test_prompt_format_multiple_message():
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
        prompt_format.generate_prompt(Prompt(prompt=[Message(**msg1), Message(**msg1)]))

    assert e.value.status_code == 400
    assert "There are 2 messages." in str(e)


def test_prompt_format_content_none():
    msg = {"role": "user", "content": None}
    with pytest.raises(ValueError):
        prompt_format.generate_prompt(Prompt(prompt=[Message(**msg)]))


def test_prompt_format_multiple_text_content():
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
        prompt_format.generate_prompt(Prompt(prompt=[Message(**msg)]))

    assert e.value.status_code == 400
    assert "The message contains more than one text type content." in str(e)


def test_prompt_format_no_text_content():
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
        prompt_format.generate_prompt(Prompt(prompt=[Message(**msg)]))

    assert e.value.status_code == 400
    assert "The message contains zero text type content." in str(e)