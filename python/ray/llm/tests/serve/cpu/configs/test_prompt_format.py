import pytest
from fastapi import HTTPException
from pydantic import ValidationError

from rayllm.models import (
    DisabledPromptFormat,
    Message,
    Prompt,
    PromptFormat,
    PromptFormatDisabledError,
)

DEFAULT_SYSTEM_MSG = "You are a helpful assistant."


def test_prompt_format_disabled():
    prompt_format = DisabledPromptFormat()
    prompt = prompt_format.generate_prompt(
        Prompt(
            prompt="hello1",
            use_prompt_format=False,
        )
    ).text
    assert prompt == "hello1"

    with pytest.raises(PromptFormatDisabledError):
        prompt_format.generate_prompt(
            Prompt(
                prompt="hello1",
                use_prompt_format=True,
            )
        )

    with pytest.raises(PromptFormatDisabledError):
        prompt_format.generate_prompt(
            Prompt(
                prompt=[Message(role="user", content="hello1")],
                use_prompt_format=False,
            )
        )

    with pytest.raises(PromptFormatDisabledError):
        prompt_format.generate_prompt(
            Prompt(
                prompt=[Message(role="user", content="hello1")],
                use_prompt_format=True,
            )
        )

    with pytest.raises(PromptFormatDisabledError):
        prompt_format.generate_prompt(
            [Message(role="user", content="hello1")],
        )


@pytest.mark.parametrize("with_bos", [True, False])
def test_prompt_format_with_prompt_obj(with_bos):
    bos = "[BOS]" if with_bos else ""
    prompt_format = PromptFormat(
        system="[system] {instruction} [/system] ",
        assistant="[assistant] {instruction} [/assistant] ",
        trailing_assistant="[assistant]",
        user="[user] {instruction} [/user] ",
        default_system_message="",
        bos=bos,
    )
    prompt = prompt_format.generate_prompt(
        Prompt(
            prompt="hello1",
            use_prompt_format=True,
        )
    ).text
    assert prompt == bos + "[user] hello1 [/user] [assistant]"
    prompt = prompt_format.generate_prompt(
        Prompt(
            prompt="hello1",
            use_prompt_format=False,
        )
    ).text
    assert prompt == bos + "hello1"


@pytest.mark.parametrize("with_bos", [True, False])
def test_prompt_format(with_bos):
    bos = "[BOS]" if with_bos else ""
    prompt_format = PromptFormat(
        system="[system] {instruction} [/system] ",
        assistant="[assistant] {instruction} [/assistant] ",
        trailing_assistant="[assistant]",
        user="[user] {instruction} [/user] ",
        default_system_message="",
        bos=bos,
    )
    # Only user, no system
    messages = [Message(role="user", content="hello1")]
    prompt = prompt_format.generate_prompt(messages).text
    assert prompt == bos + "[user] hello1 [/user] [assistant]"

    # User+system
    messages = [
        Message(role="system", content="hello1"),
        Message(role="user", content="hello2"),
    ]
    prompt = prompt_format.generate_prompt(messages).text
    assert prompt == bos + "[system] hello1 [/system] [user] hello2 [/user] [assistant]"

    # User+assistant+user
    messages = [
        Message(role="user", content="hello1"),
        Message(role="assistant", content="hello2"),
        Message(role="user", content="hello3"),
    ]
    prompt = prompt_format.generate_prompt(messages).text
    assert (
        prompt
        == bos
        + "[user] hello1 [/user] [assistant] hello2 [/assistant] [user] hello3 [/user] [assistant]"
    )

    # system+User+assistant+user
    messages = [
        Message(role="system", content="hello1"),
        Message(role="user", content="hello2"),
        Message(role="assistant", content="hello3"),
        Message(role="user", content="hello4"),
    ]
    prompt = prompt_format.generate_prompt(messages).text
    assert (
        prompt
        == bos
        + "[system] hello1 [/system] [user] hello2 [/user] [assistant] hello3 [/assistant] [user] hello4 [/user] [assistant]"
    )

    # User+assistant+user+assistant+user
    messages = [
        Message(role="user", content="hello1"),
        Message(role="assistant", content="hello2"),
        Message(role="user", content="hello3"),
        Message(role="assistant", content="hello4"),
        Message(role="user", content="hello5"),
    ]
    prompt = prompt_format.generate_prompt(messages).text
    assert (
        prompt
        == bos
        + "[user] hello1 [/user] [assistant] hello2 [/assistant] [user] hello3 [/user] [assistant] hello4 [/assistant] [user] hello5 [/user] [assistant]"
    )

    # system+User+assistant+user+assistant+user
    messages = [
        Message(role="system", content="hello1"),
        Message(role="user", content="hello2"),
        Message(role="assistant", content="hello3"),
        Message(role="user", content="hello4"),
        Message(role="assistant", content="hello5"),
        Message(role="user", content="hello6"),
    ]
    prompt = prompt_format.generate_prompt(messages).text
    assert (
        prompt
        == bos
        + "[system] hello1 [/system] [user] hello2 [/user] [assistant] hello3 [/assistant] [user] hello4 [/user] [assistant] hello5 [/assistant] [user] hello6 [/user] [assistant]"
    )


def test_prompt_format_with_image():
    message_data = {
        "role": "user",
        "content": [
            {
                "type": "text",
                "text": "What is the content of this image?",
            },
            {
                "type": "image_url",
                "image_url": {
                    "url": "https://foo.jpeg",
                },
            },
        ],
    }
    prompt_format = PromptFormat(
        system="[system] {instruction} [/system] ",
        assistant="[assistant] {instruction} [/assistant] ",
        trailing_assistant="[assistant]",
        user="[user] {instruction} [/user] ",
        default_system_message="",
    )
    messages = [
        Message(**message_data),
    ]
    with pytest.raises(HTTPException) as e:
        prompt_format.generate_prompt(messages)

    assert e.value.status_code == 400


def test_prompt_format_default_system_message():
    prompt_format = PromptFormat(
        system="[system] {instruction} [/system] ",
        assistant="[assistant] {instruction} [/assistant] ",
        trailing_assistant="[assistant]",
        user="[user] {instruction} [/user] ",
        default_system_message="Test",
    )
    # Only user, no system
    messages = [Message(role="user", content="hello1")]
    prompt = prompt_format.generate_prompt(messages).text
    assert prompt == "[system] Test [/system] [user] hello1 [/user] [assistant]"

    # User+system
    messages = [
        Message(role="system", content="hello1"),
        Message(role="user", content="hello2"),
    ]
    prompt = prompt_format.generate_prompt(messages).text
    assert prompt == "[system] hello1 [/system] [user] hello2 [/user] [assistant]"

    # User+assistant+user
    messages = [
        Message(role="user", content="hello1"),
        Message(role="assistant", content="hello2"),
        Message(role="user", content="hello3"),
    ]
    prompt = prompt_format.generate_prompt(messages).text
    assert (
        prompt
        == "[system] Test [/system] [user] hello1 [/user] [assistant] hello2 [/assistant] [user] hello3 [/user] [assistant]"
    )

    # system+User+assistant+user
    messages = [
        Message(role="system", content="hello1"),
        Message(role="user", content="hello2"),
        Message(role="assistant", content="hello3"),
        Message(role="user", content="hello4"),
    ]
    prompt = prompt_format.generate_prompt(messages).text
    assert (
        prompt
        == "[system] hello1 [/system] [user] hello2 [/user] [assistant] hello3 [/assistant] [user] hello4 [/user] [assistant]"
    )

    # User+assistant+user+assistant+user
    messages = [
        Message(role="user", content="hello1"),
        Message(role="assistant", content="hello2"),
        Message(role="user", content="hello3"),
        Message(role="assistant", content="hello4"),
        Message(role="user", content="hello5"),
    ]
    prompt = prompt_format.generate_prompt(messages).text
    assert (
        prompt
        == "[system] Test [/system] [user] hello1 [/user] [assistant] hello2 [/assistant] [user] hello3 [/user] [assistant] hello4 [/assistant] [user] hello5 [/user] [assistant]"
    )

    # system+User+assistant+user+assistant+user
    messages = [
        Message(role="system", content="hello1"),
        Message(role="user", content="hello2"),
        Message(role="assistant", content="hello3"),
        Message(role="user", content="hello4"),
        Message(role="assistant", content="hello5"),
        Message(role="user", content="hello6"),
    ]
    prompt = prompt_format.generate_prompt(messages).text
    assert (
        prompt
        == "[system] hello1 [/system] [user] hello2 [/user] [assistant] hello3 [/assistant] [user] hello4 [/user] [assistant] hello5 [/assistant] [user] hello6 [/user] [assistant]"
    )

    # system+system+user
    # should error
    messages = [
        Message(role="system", content="hello"),
        Message(role="user", content="hello"),
        Message(role="system", content="hello"),
    ]
    with pytest.raises(HTTPException) as e:
        prompt = prompt_format.generate_prompt(messages)

    assert e.value.status_code == 400

    # user+system+assistant
    # system should be moved to top
    messages = [
        Message(role="user", content="hello1"),
        Message(role="system", content="hello2"),
    ]
    prompt = prompt_format.generate_prompt(messages).text
    assert prompt == "[system] hello2 [/system] [user] hello1 [/user] [assistant]"


def test_prompt_format_system_in_user():
    with pytest.raises(ValidationError):
        # Should raise if system_in_user=True and
        # user doesn't have '{system}'
        prompt_format = PromptFormat(
            system="[system] {instruction} [/system] ",
            assistant="[assistant] {instruction} [/assistant] ",
            trailing_assistant="[assistant]",
            user="[user] {instruction} [/user] ",
            default_system_message="",
            system_in_user=True,
        )

    prompt_format = PromptFormat(
        system="<<SYS>>\n{instruction}\n<</SYS>>\n\n",
        assistant=" {instruction} </s><s> ",
        trailing_assistant=" ",
        user="[INST] {system}{instruction} [/INST]",
        default_system_message="",
        system_in_user=True,
    )

    # Only user, no system
    messages = [Message(role="user", content="hello1")]
    prompt = prompt_format.generate_prompt(messages).text
    assert prompt == "[INST] hello1 [/INST] "

    # User+system
    messages = [
        Message(role="system", content="hello1"),
        Message(role="user", content="hello2"),
    ]
    prompt = prompt_format.generate_prompt(messages).text
    assert prompt == "[INST] <<SYS>>\nhello1\n<</SYS>>\n\nhello2 [/INST] "

    # User+assistant+user
    messages = [
        Message(role="user", content="hello1"),
        Message(role="assistant", content="hello2"),
        Message(role="user", content="hello3"),
    ]
    prompt = prompt_format.generate_prompt(messages).text
    assert prompt == "[INST] hello1 [/INST] hello2 </s><s> [INST] hello3 [/INST] "

    # system+User+assistant+user
    messages = [
        Message(role="system", content="hello1"),
        Message(role="user", content="hello2"),
        Message(role="assistant", content="hello3"),
        Message(role="user", content="hello4"),
    ]
    prompt = prompt_format.generate_prompt(messages).text
    assert (
        prompt
        == "[INST] <<SYS>>\nhello1\n<</SYS>>\n\nhello2 [/INST] hello3 </s><s> [INST] hello4 [/INST] "
    )

    # User+assistant+user+assistant+user
    messages = [
        Message(role="user", content="hello1"),
        Message(role="assistant", content="hello2"),
        Message(role="user", content="hello3"),
        Message(role="assistant", content="hello4"),
        Message(role="user", content="hello5"),
    ]
    prompt = prompt_format.generate_prompt(messages).text
    assert (
        prompt
        == "[INST] hello1 [/INST] hello2 </s><s> [INST] hello3 [/INST] hello4 </s><s> [INST] hello5 [/INST] "
    )

    # system+User+assistant+user+assistant+user
    messages = [
        Message(role="system", content="hello1"),
        Message(role="user", content="hello2"),
        Message(role="assistant", content="hello3"),
        Message(role="user", content="hello4"),
        Message(role="assistant", content="hello5"),
        Message(role="user", content="hello6"),
    ]
    prompt = prompt_format.generate_prompt(messages).text
    assert (
        prompt
        == "[INST] <<SYS>>\nhello1\n<</SYS>>\n\nhello2 [/INST] hello3 </s><s> [INST] hello4 [/INST] hello5 </s><s> [INST] hello6 [/INST] "
    )

    # user+system+assistant
    # system should be moved to top
    messages = [
        Message(role="user", content="hello1"),
        Message(role="system", content="hello2"),
    ]
    prompt = prompt_format.generate_prompt(messages).text
    assert prompt == "[INST] <<SYS>>\nhello2\n<</SYS>>\n\nhello1 [/INST] "


def test_prompt_format_system_in_user_default_system_message():
    with pytest.raises(ValidationError):
        # Should raise if system_in_user=True and
        # user doesn't have '{system}'
        prompt_format = PromptFormat(
            system="[system] {instruction} [/system] ",
            assistant="[assistant] {instruction} [/assistant] ",
            trailing_assistant="[assistant]",
            user="[user] {instruction} [/user] ",
            default_system_message="",
            system_in_user=True,
        )

    prompt_format = PromptFormat(
        system="<<SYS>>\n{instruction}\n<</SYS>>\n\n",
        assistant=" {instruction} </s><s> ",
        trailing_assistant=" ",
        user="[INST] {system}{instruction} [/INST]",
        default_system_message="Test",
        system_in_user=True,
    )

    # Only user, no system
    messages = [Message(role="user", content="hello1")]
    prompt = prompt_format.generate_prompt(messages).text
    assert prompt == "[INST] <<SYS>>\nTest\n<</SYS>>\n\nhello1 [/INST] "

    # User+system
    messages = [
        Message(role="system", content="hello1"),
        Message(role="user", content="hello2"),
    ]
    prompt = prompt_format.generate_prompt(messages).text
    assert prompt == "[INST] <<SYS>>\nhello1\n<</SYS>>\n\nhello2 [/INST] "

    # User+assistant+user
    messages = [
        Message(role="user", content="hello1"),
        Message(role="assistant", content="hello2"),
        Message(role="user", content="hello3"),
    ]
    prompt = prompt_format.generate_prompt(messages).text
    assert (
        prompt
        == "[INST] <<SYS>>\nTest\n<</SYS>>\n\nhello1 [/INST] hello2 </s><s> [INST] hello3 [/INST] "
    )

    # system+User+assistant+user
    messages = [
        Message(role="system", content="hello1"),
        Message(role="user", content="hello2"),
        Message(role="assistant", content="hello3"),
        Message(role="user", content="hello4"),
    ]
    prompt = prompt_format.generate_prompt(messages).text
    assert (
        prompt
        == "[INST] <<SYS>>\nhello1\n<</SYS>>\n\nhello2 [/INST] hello3 </s><s> [INST] hello4 [/INST] "
    )

    # User+assistant+user+assistant+user
    messages = [
        Message(role="user", content="hello1"),
        Message(role="assistant", content="hello2"),
        Message(role="user", content="hello3"),
        Message(role="assistant", content="hello4"),
        Message(role="user", content="hello5"),
    ]
    prompt = prompt_format.generate_prompt(messages).text
    assert (
        prompt
        == "[INST] <<SYS>>\nTest\n<</SYS>>\n\nhello1 [/INST] hello2 </s><s> [INST] hello3 [/INST] hello4 </s><s> [INST] hello5 [/INST] "
    )

    # system+User+assistant+user+assistant+user
    messages = [
        Message(role="system", content="hello1"),
        Message(role="user", content="hello2"),
        Message(role="assistant", content="hello3"),
        Message(role="user", content="hello4"),
        Message(role="assistant", content="hello5"),
        Message(role="user", content="hello6"),
    ]
    prompt = prompt_format.generate_prompt(messages).text
    assert (
        prompt
        == "[INST] <<SYS>>\nhello1\n<</SYS>>\n\nhello2 [/INST] hello3 </s><s> [INST] hello4 [/INST] hello5 </s><s> [INST] hello6 [/INST] "
    )

    # user+system+assistant
    # system should be moved to top
    messages = [
        Message(role="user", content="hello1"),
        Message(role="system", content="hello2"),
    ]
    prompt = prompt_format.generate_prompt(messages).text
    assert prompt == "[INST] <<SYS>>\nhello2\n<</SYS>>\n\nhello1 [/INST] "


def test_prompt_format_add_system_tags():
    prompt_format = PromptFormat(
        system="[system] {instruction} [/system] ",
        assistant="[assistant] {instruction} [/assistant] ",
        trailing_assistant="[assistant]",
        user="[user] {instruction} [/user] ",
        default_system_message="",
        add_system_tags_even_if_message_is_empty=True,
    )

    # Only user, no system
    messages = [Message(role="user", content="hello1")]
    prompt = prompt_format.generate_prompt(messages).text
    assert prompt == "[system]  [/system] [user] hello1 [/user] [assistant]"

    prompt_format = PromptFormat(
        system="<<SYS>>\n{instruction}\n<</SYS>>\n\n",
        assistant=" {instruction} </s><s> ",
        trailing_assistant=" ",
        user="[INST] {system}{instruction} [/INST]",
        default_system_message="",
        system_in_user=True,
        add_system_tags_even_if_message_is_empty=True,
    )

    # Only user, no system
    messages = [Message(role="user", content="hello1")]
    prompt = prompt_format.generate_prompt(messages).text
    assert prompt == "[INST] <<SYS>>\n\n<</SYS>>\n\nhello1 [/INST] "


def test_prompt_format_strip_whitespace():
    prompt_format = PromptFormat(
        system="[system] {instruction} [/system] ",
        assistant="[assistant] {instruction} [/assistant] ",
        trailing_assistant="[assistant]",
        user="[user] {instruction} [/user] ",
        default_system_message="",
        strip_whitespace=True,
    )

    # Only user, no system
    messages = [
        Message(role="user", content="hello1 "),
        Message(role="assistant", content=" hello2"),
        Message(role="user", content="hello3"),
    ]
    prompt = prompt_format.generate_prompt(messages).text
    assert (
        prompt
        == "[user] hello1 [/user] [assistant] hello2 [/assistant] [user] hello3 [/user] [assistant]"
    )

    prompt_format = PromptFormat(
        system="[system] {instruction} [/system] ",
        assistant="[assistant] {instruction} [/assistant] ",
        trailing_assistant="[assistant]",
        user="[user] {instruction} [/user] ",
        default_system_message="",
        strip_whitespace=False,
    )

    # Only user, no system
    messages = [
        Message(role="user", content="hello1 "),
        Message(role="assistant", content=" hello2"),
        Message(role="user", content="hello3"),
    ]
    prompt = prompt_format.generate_prompt(messages).text
    assert (
        prompt
        == "[user] hello1  [/user] [assistant]  hello2 [/assistant] [user] hello3 [/user] [assistant]"
    )


@pytest.mark.parametrize("model_id", ["meta-llama/Llama-2-7b-chat-hf"])
def test_prompt_format_equivalency_llama(tokenizer):
    prompt_format = PromptFormat(
        system="<<SYS>>\n{instruction}\n<</SYS>>\n\n",
        assistant=" {instruction} </s><s>",
        trailing_assistant="",
        user="[INST] {system}{instruction} [/INST]",
        default_system_message="",
        system_in_user=True,
    )

    default_chat_template = (
        "{% if messages[0]['role'] == 'system' %}"
        "{% set loop_messages = messages[1:] %}"
        "{% set system_message = messages[0]['content'] %}"
        "{% elif false == true and not '<<SYS>>' in messages[0]['content'] %}"
        "{% set loop_messages = messages %}"
        "{% set system_message = 'You are a helpful, respectful and honest assistant. "
        "Always answer as helpfully as possible, while being safe. Your answers should "
        "not include any harmful, unethical, racist, sexist, toxic, dangerous, or illegal content. "
        "Please ensure that your responses are socially unbiased and positive in nature."
        "\\n\\nIf a question does not make any sense, or is not factually coherent, explain why "
        "instead of answering something not correct. If you don\\'t know the answer to a question, "
        "please don\\'t share false information.' %}{% else %}{% set loop_messages = messages %}"
        "{% set system_message = false %}{% endif %}{% for message in loop_messages %}"
        "{% if (message['role'] == 'user') != (loop.index0 % 2 == 0) %}"
        "{{ raise_exception('Conversation roles must alternate user/assistant/user/assistant/...') }}"
        "{% endif %}{% if loop.index0 == 0 and system_message != false %}"
        "{% set content = '<<SYS>>\\n' + system_message + '\\n<</SYS>>\\n\\n' + message['content'] %}"
        "{% else %}{% set content = message['content'] %}{% endif %}{% if message['role'] == 'user' %}"
        "{{ bos_token + '[INST] ' + content.strip() + ' [/INST]' }}"
        "{% elif message['role'] == 'system' %}{{ '<<SYS>>\\n' + content.strip() + '\\n<</SYS>>\\n\\n' }}"
        "{% elif message['role'] == 'assistant' %}{{ ' '  + content.strip() + ' ' + eos_token }}"
        "{% endif %}{% endfor %}"
    )

    conversations = [
        [Message(role="user", content="hello1")],
        [
            Message(role="system", content="hello1"),
            Message(role="user", content="hello2"),
        ],
        [
            Message(role="user", content="hello1"),
            Message(role="assistant", content="hello2"),
            Message(role="user", content="hello3"),
        ],
        [
            Message(role="system", content="hello1"),
            Message(role="user", content="hello2"),
            Message(role="assistant", content="hello3"),
            Message(role="user", content="hello4"),
        ],
        [
            Message(role="user", content="hello1"),
            Message(role="assistant", content="hello2"),
            Message(role="user", content="hello3"),
            Message(role="assistant", content="hello4"),
            Message(role="user", content="hello5"),
        ],
        [
            Message(role="system", content="hello1"),
            Message(role="user", content="hello2"),
            Message(role="assistant", content="hello3"),
            Message(role="user", content="hello4"),
            Message(role="assistant", content="hello5"),
            Message(role="user", content="hello6"),
        ],
    ]

    for conversation in conversations:
        dict_conversation = [message.model_dump() for message in conversation]
        reference_tokens = tokenizer.apply_chat_template(
            dict_conversation,
            tokenize=True,
            chat_template=default_chat_template,
        )
        our_tokens = tokenizer.encode(prompt_format.generate_prompt(conversation).text)
        assert reference_tokens == our_tokens


@pytest.mark.parametrize("model_id", ["mistralai/Mistral-7B-Instruct-v0.1"])
def test_prompt_format_equivalency_mistral(tokenizer):
    prompt_format = PromptFormat(
        system="{instruction} + ",
        assistant="{instruction}</s> ",
        trailing_assistant="",
        user="[INST] {system}{instruction} [/INST]",
        default_system_message="",
        system_in_user=True,
    )

    conversations = [
        [Message(role="user", content="hello1")],
        [
            Message(role="user", content="hello1"),
            Message(role="assistant", content="hello2"),
            Message(role="user", content="hello3"),
        ],
        [
            Message(role="user", content="hello2"),
            Message(role="assistant", content="hello3"),
            Message(role="user", content="hello4"),
        ],
        [
            Message(role="user", content="hello1"),
            Message(role="assistant", content="hello2"),
            Message(role="user", content="hello3"),
            Message(role="assistant", content="hello4"),
            Message(role="user", content="hello5"),
        ],
    ]
    for conversation in conversations:
        dict_conversation = [message.model_dump() for message in conversation]
        reference_tokens = tokenizer.apply_chat_template(
            dict_conversation, tokenize=True
        )
        our_tokens = tokenizer.encode(prompt_format.generate_prompt(conversation).text)
        assert reference_tokens == our_tokens


@pytest.mark.parametrize("model_id", ["HuggingFaceH4/zephyr-7b-beta"])
def test_prompt_format_equivalency_zephyr(tokenizer):
    prompt_format = PromptFormat(
        system="<|system|>\n{instruction}</s>\n",
        assistant="<|assistant|>\n{instruction}</s>\n",
        trailing_assistant="<|assistant|>\n",
        user="<|user|>\n{instruction}</s>\n",
        default_system_message="",
        system_in_user=False,
    )

    conversations = [
        [Message(role="user", content="hello1")],
        [
            Message(role="system", content="hello1"),
            Message(role="user", content="hello2"),
        ],
        [
            Message(role="user", content="hello1"),
            Message(role="assistant", content="hello2"),
            Message(role="user", content="hello3"),
        ],
        [
            Message(role="system", content="hello1"),
            Message(role="user", content="hello2"),
            Message(role="assistant", content="hello3"),
            Message(role="user", content="hello4"),
        ],
        [
            Message(role="user", content="hello1"),
            Message(role="assistant", content="hello2"),
            Message(role="user", content="hello3"),
            Message(role="assistant", content="hello4"),
            Message(role="user", content="hello5"),
        ],
        [
            Message(role="system", content="hello1"),
            Message(role="user", content="hello2"),
            Message(role="assistant", content="hello3"),
            Message(role="user", content="hello4"),
            Message(role="assistant", content="hello5"),
            Message(role="user", content="hello6"),
        ],
    ]
    for conversation in conversations:
        dict_conversation = [message.model_dump() for message in conversation]
        reference_tokens = tokenizer.apply_chat_template(
            dict_conversation, tokenize=True, add_generation_prompt=True
        )
        our_tokens = tokenizer.encode(
            prompt_format.generate_prompt(conversation).text, add_special_tokens=False
        )
        assert reference_tokens == our_tokens


@pytest.mark.parametrize("model_id", ["codellama/CodeLlama-70b-Instruct-hf"])
def test_prompt_format_equivalency_codellama_70b(tokenizer):
    prompt_format = PromptFormat(
        system="Source: system\n\n {instruction} <step> ",
        assistant="Source: assistant\n\n {instruction} <step> ",
        trailing_assistant="Source: assistant\nDestination: user\n\n ",
        user="Source: user\n\n {instruction} <step> ",
        default_system_message="",
        system_in_user=False,
    )

    conversations = [
        [Message(role="user", content="hello1")],
        [
            Message(role="system", content="hello1"),
            Message(role="user", content="hello2"),
        ],
        [
            Message(role="user", content="hello1"),
            Message(role="assistant", content="hello2"),
            Message(role="user", content="hello3"),
        ],
        [
            Message(role="system", content="hello1"),
            Message(role="user", content="hello2"),
            Message(role="assistant", content="hello3"),
            Message(role="user", content="hello4"),
        ],
        [
            Message(role="user", content="hello1"),
            Message(role="assistant", content="hello2"),
            Message(role="user", content="hello3"),
            Message(role="assistant", content="hello4"),
            Message(role="user", content="hello5"),
        ],
        [
            Message(role="system", content="hello1"),
            Message(role="user", content="hello2"),
            Message(role="assistant", content="hello3"),
            Message(role="user", content="hello4"),
            Message(role="assistant", content="hello5"),
            Message(role="user", content="hello6"),
        ],
    ]
    for conversation in conversations:
        dict_conversation = [message.model_dump() for message in conversation]
        reference_tokens = tokenizer.apply_chat_template(
            dict_conversation, tokenize=True, add_generation_prompt=True
        )
        our_tokens = tokenizer.encode(prompt_format.generate_prompt(conversation).text)
        assert reference_tokens == our_tokens


@pytest.mark.parametrize("model_id", ["google/gemma-7b-it"])
def test_prompt_format_equivalency_gemma(tokenizer):
    prompt_format = PromptFormat(
        system="{instruction}\n",
        assistant="<start_of_turn>model\n{instruction}<end_of_turn>\n",
        trailing_assistant="<start_of_turn>model\n",
        user="<start_of_turn>user\n{system}{instruction}<end_of_turn>\n",
        default_system_message="",
        system_in_user=True,
    )

    conversations = [
        [Message(role="user", content="hello1")],
        [
            Message(role="user", content="hello2"),
        ],
        [
            Message(role="user", content="hello1"),
            Message(role="assistant", content="hello2"),
            Message(role="user", content="hello3"),
        ],
        [
            Message(role="user", content="hello2"),
            Message(role="assistant", content="hello3"),
            Message(role="user", content="hello4"),
        ],
        [
            Message(role="user", content="hello2"),
            Message(role="assistant", content="hello3"),
            Message(role="user", content="hello4"),
            Message(role="assistant", content="hello5"),
            Message(role="user", content="hello6"),
        ],
    ]
    for conversation in conversations:
        dict_conversation = [message.model_dump() for message in conversation]
        reference_tokens = tokenizer.apply_chat_template(
            dict_conversation, tokenize=True, add_generation_prompt=True
        )
        # Add the BOS token
        if not reference_tokens[0] == 2:
            reference_tokens = [2] + reference_tokens
        our_tokens = tokenizer.encode(prompt_format.generate_prompt(conversation).text)
        assert reference_tokens == our_tokens