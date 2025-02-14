# This file asserts the consistency between ray-llm and llm-forge.
import pytest

from ray.llm._internal.serve.configs.prompt_formats import Message, PromptFormat

# These are copied from the asserted outputs from `llm-forge/tests/core/data/test_prompt_format_consistency.py`.
llama2_prompt = "[INST] <<SYS>>\nhello1\n<</SYS>>\n\nhello2 [/INST] hello3 </s><s>[INST] hello4 [/INST] hello5 </s><s>"
mistral_prompt = (
    "[INST] hello1 + hello2 [/INST]hello3</s> [INST] hello4 [/INST]hello5</s> "
)

# These are copied from `llm-forge/llmforge/config_files/endpoints/endpoint_configs.yaml`.
llama2_prompt_format = PromptFormat(
    system="<<SYS>>\n{instruction}\n<</SYS>>\n\n",
    assistant=" {instruction} </s><s>",
    trailing_assistant="",
    user="[INST] {system}{instruction} [/INST]",
    default_system_message="",
    system_in_user=True,
)
mistral_prompt_format = PromptFormat(
    system="{instruction} + ",
    assistant="{instruction}</s> ",
    trailing_assistant="",
    user="[INST] {system}{instruction} [/INST]",
    default_system_message="",
    system_in_user=True,
)

messages = [
    {"role": "system", "content": "hello1"},
    {"role": "user", "content": "hello2"},
    {"role": "assistant", "content": "hello3"},
    {"role": "user", "content": "hello4"},
]


# llm-forge trains with messages like s, u, a, u, a
# ray-llm infers with messages like s, u, a, u
# They are not exactly the same. The latter yields a prompt that's a subsequence of
# the former, thus assert with `startswith`.
@pytest.mark.parametrize(
    ("prompt_format", "expected_ft_prompt"),
    [(llama2_prompt_format, llama2_prompt), (mistral_prompt_format, mistral_prompt)],
)
def test_prompt_format(prompt_format, expected_ft_prompt):
    inference_prompt = prompt_format.generate_prompt(
        messages=[Message(**m) for m in messages]
    ).text
    assert expected_ft_prompt.startswith(inference_prompt)
