from typing import List, Sequence, Type

import backoff
import openai
from openai import APIStatusError
from openai._models import BaseModel

# NOTE: Currently, by default we're not retrying any exceptions
# For context please check out: https://github.com/anyscale/ray-llm/pull/1028/files#r1448169807
DEFAULT_RETRYABLE_EXCEPTIONS = ()
DEFAULT_MAX_ATTEMPTS = 2


def _apply_delta(base, delta):
    """Recursively merges the changes from 'delta' into 'base'.

    Strings are concatenated, numbers are treated as separate nodes and returned as a list, and None is ignored.
    """

    if delta is None:
        return base

    assert type(base) is type(
        delta
    ), f"type mismatch between base {type(base)} and delta {type(delta)}"

    # This flag is used to convert the results back to list if necessary
    # We treat lists as dictionaries with integer keys
    convert_to_list = False
    if isinstance(base, list):
        base = {base[i]["index"]: base[i] for i in range(len(base))}
        delta = {delta[i]["index"]: delta[i] for i in range(len(delta))}

        # In the end we need to convert the results back to list
        convert_to_list = True

    for key in base:
        if key not in delta:
            continue

        # logprobs is a special case, we need to concatenate logprobs content
        # in order to merge them, not recursively merge them.
        if key == "logprobs":
            if delta[key]:
                base[key]["content"].extend(delta[key]["content"])
            continue

        if isinstance(base[key], dict):
            base[key] = _apply_delta(base[key], delta[key])
        elif isinstance(base[key], list):
            base[key] = _apply_delta(base[key], delta[key])
        elif isinstance(base[key], str):
            assert (
                isinstance(delta[key], str) or delta[key] is None
            ), f"type mismatch on key = {key}"
            base[key] += delta[key] or ""
        elif isinstance(base[key], (int, float)):
            continue
        elif base[key] is None:
            base[key] = delta[key]

    for key in delta:
        if key not in base:
            base[key] = delta[key]

    if convert_to_list:
        base = [base[idx] for idx in sorted(base)]
        delta = [delta[idx] for idx in sorted(delta)]

    return base


def apply_delta_changes(delta_list):
    """Applies a list of delta changes to construct the final data structure."""
    deltas = {}
    for item in delta_list:
        if item["index"] not in deltas:
            deltas[item["index"]] = item
        else:
            deltas[item["index"]] = _apply_delta(deltas[item["index"]], item)
    final_results = []
    for key in deltas:
        result = deltas[key]
        if "delta" in result:
            result["message"] = result["delta"]
            del result["delta"]
        final_results.append(result)

    return final_results


class TextGenerationProbeResponse:
    def __init__(self, response=List[BaseModel]):
        self.response = response

    def messages(self):
        """In case of streamed response, what are the individual chunked messages? that contain the content we care about?"""
        vals = []
        for r in self.response:
            v = r.choices[0].model_dump()
            if "message" in v and "content" in v["message"]:
                vals.append(v["message"]["content"] or "")
            elif "delta" in v and "content" in v["delta"]:
                vals.append(v["delta"]["content"] or "")
        return vals

    def messages_dicts(self):
        vals = []
        for r in self.response:
            for choice in r.choices:
                vals.append(choice.model_dump())
        return vals

    def full_dict(self):
        messages_dicts = self.messages_dicts()
        return apply_delta_changes(messages_dicts)

    def full(self) -> str:
        """In case of streamed response, what is the full response by concatenating individual responses?"""
        return "".join(self.messages())

    def num_completion_tokens(self):
        # Usage is set on the last element in the stream
        try:
            return self.response[-1].usage.completion_tokens
        except AttributeError:
            return self.response[-1].usage.get("completion_tokens")

    def finish_reason(self):
        # This should be set on the last response.
        return self.response[-1].choices[0].finish_reason


class BaseProbe:
    def __init__(
        self,
        client: openai.AsyncClient,
        retryable_error_types: Sequence[Type[APIStatusError]] = None,
    ):
        assert not client or isinstance(
            client, openai.AsyncClient
        ), "Async OpenAI client is expected!"
        self.client: openai.AsyncClient = client
        self.retryable_error_types: Sequence[Type[APIStatusError]] = (
            retryable_error_types
            if retryable_error_types is not None
            else DEFAULT_RETRYABLE_EXCEPTIONS
        )


class TextGenerationProbeQuerier(BaseProbe):
    def __init__(
        self,
        client: openai.AsyncClient,
        default_configuration=None,
        retryable_error_types: Sequence[Type[APIStatusError]] = None,
    ):
        super().__init__(client, retryable_error_types)
        self.default_configuration = default_configuration or {}

    async def query(
        self,
        model: str,
        stream: bool = False,
        chat: bool = True,
        **chat_args,
    ):
        args = {
            **self.default_configuration,
            "model": model,
            "stream": stream,
            **chat_args,
        }
        if chat:
            method = self.client.chat.completions.create
        else:
            method = self.client.completions.create

        method = backoff.on_exception(
            backoff.constant,
            self.retryable_error_types,
            max_tries=DEFAULT_MAX_ATTEMPTS,
        )(method)

        res = await method(**args)
        wrapped_response = [v async for v in res] if stream else [res]
        return TextGenerationProbeResponse(wrapped_response)
