import asyncio
import json
from typing import List

import openai
import pytest
from pydantic import BaseModel, Field

from probes.messages import messages, system, user
from probes.models import ModelLoader
from probes.query_utils import TextGenerationProbeQuerier

MODEL_IDS = ModelLoader().json_mode_model_ids()

# coming from
# https://github.com/mlc-ai/xgrammar/blob/5e141f6ff1ca02bc31f9e512e68b61f2a8ae88e5/docs/how_to/ebnf_guided_generation.rst?plain=1#L158
JSON_GRAMMAR_EBNF_STR = r"""
root ::= basic_array | basic_object
basic_any ::= basic_number | basic_string | basic_boolean | basic_null | basic_array | basic_object
basic_integer ::= ("0" | "-"? [1-9] [0-9]*) ".0"?
basic_number ::= ("0" | "-"? [1-9] [0-9]*) ("." [0-9]+)? ([eE] [+-]? [0-9]+)?
basic_string ::= (([\"] basic_string_1 [\"]))
basic_string_1 ::= "" | [^"\\\x00-\x1F] basic_string_1 | "\\" escape basic_string_1
escape ::= ["\\/bfnrt] | "u" [A-Fa-f0-9] [A-Fa-f0-9] [A-Fa-f0-9] [A-Fa-f0-9]
basic_boolean ::= "true" | "false"
basic_null ::= "null"
basic_array ::= "[" ("" | ws basic_any (ws "," ws basic_any)*) ws "]"
basic_object ::= "{" ("" | ws basic_string ws ":" ws basic_any ( ws "," ws basic_string ws ":" ws basic_any)*) ws "}"
ws ::= [ \n\t]*
"""


class BasicResponse(BaseModel):
    """The format of the answer."""

    winner_team_name: str = Field(description="Name of the winning team")
    loser_team_name: str = Field(description="Name of the losing team")
    winner_score: int = Field(description="Score of the winning team")
    loser_score: int = Field(description="Score of the losing team")


class ArrayResponse(BaseModel):
    """The format of the answer."""

    sorted_numbers: List[int] = Field(description="List of the sorted numbers")


class Person(BaseModel):
    """The object representing a person with name and age"""

    name: str = Field(description="Name of the person")
    age: int = Field(description="The age of the person")


class NestedResponse(BaseModel):
    """The format of the answer."""

    sorted_list: List[Person] = Field(description="List of the sorted objects")


def get_params_and_expected_type(response_type: str):
    params = {}
    if response_type == "basic":
        params.update(
            **messages(
                system("You are a helpful assistant designed to output JSON."),
                user("Who won the world series in 2020?"),
            )
        )
        expected_type = BasicResponse
    elif response_type == "array":
        params.update(
            **messages(
                system("You are a helpful assistant designed to output JSON."),
                user("Sort the numbers 3, 1, 2, 4, 5"),
            )
        )
        expected_type = ArrayResponse
    elif response_type == "nested":
        params.update(
            **messages(
                system("You are a helpful assistant designed to output JSON."),
                user(
                    "Sort these people by age: John, 20 years old, Mary, 30 years old, Bob, 10 years old."
                ),
            )
        )
        expected_type = NestedResponse
    else:
        raise ValueError(
            f"Unknown response type {response_type} only basic, array, and nested are supported"
        )

    params.update(
        {
            "response_format": {
                "type": "json_object",
                "schema": expected_type.schema_json(),
            }
        }
    )

    return params, expected_type


def get_response_formats():
    return [
        # TODO (Kourosh): The following should be supported
        {"type": "json_object"},
        {"type": "json_object", "schema": {}},
        {"type": "json_object", "schema": json.dumps({})},
        {"type": "json_object", "schema": json.loads(BasicResponse.schema_json())},
        {"type": "json_object", "schema": BasicResponse.schema_json()},
        {"type": "grammar", "grammar": JSON_GRAMMAR_EBNF_STR},
    ]


async def query_json_model(
    model: str, response_type: str, stream: bool, openai_async_client
):
    querier = TextGenerationProbeQuerier(openai_async_client, {"temperature": 0.0})

    params, expected_type = get_params_and_expected_type(response_type)
    response = await querier.query(model, stream=stream, **params)
    response_str = response.full()

    return response_str, expected_type


@pytest.mark.asyncio
@pytest.mark.parametrize("model", MODEL_IDS)
# @pytest.mark.parametrize("response_type", ["basic", "array", "nested"])
@pytest.mark.parametrize("response_type", ["basic"])
@pytest.mark.parametrize("n_concurrent_requests", [3])
# @pytest.mark.parametrize("stream", [True, False])
@pytest.mark.parametrize("stream", [False])
async def test_json_mode(
    test_id: str,
    model: str,
    response_type: str,
    n_concurrent_requests: int,
    stream: bool,
    openai_async_client,
):
    print(
        f"Sending json mode request to {model} ({test_id}) with {n_concurrent_requests} concurrent requests"
    )

    responses = await asyncio.gather(
        *[
            query_json_model(model, response_type, stream, openai_async_client)
            for _ in range(n_concurrent_requests)
        ]
    )

    for response_str, expected_type in responses:
        # Note: We just care about the returned object getting parsed into the correct type, The model may or may not have solved the task correctly
        print(f"{response_str=}")
        expected_type(**json.loads(response_str))


@pytest.mark.parametrize("model", MODEL_IDS)
@pytest.mark.parametrize("response_format", get_response_formats())
@pytest.mark.parametrize("stream", [True, False])
@pytest.mark.asyncio
async def test_response_format_options(
    test_id: str, model: str, response_format: dict, stream: bool, openai_async_client
):
    querier = TextGenerationProbeQuerier(
        openai_async_client, {"temperature": 0.0, "max_tokens": 1024}
    )

    params = {
        **messages(
            system("You are a helpful assistant designed to output JSON."),
            user("Who won the world series in 2020?"),
        ),
        "response_format": response_format,
    }

    print(f"({test_id}) Sending request with response_format {response_format}")
    response = await querier.query(model, stream=stream, **params)
    print(f"{response.full()=}")


@pytest.mark.parametrize("model", MODEL_IDS)
@pytest.mark.asyncio
async def test_invalid_schema(model: str, openai_async_client):
    querier = TextGenerationProbeQuerier(openai_async_client, {"temperature": 0.0})
    response_format = {
        "type": "json_object",
        "schema": {"type": "object", "properties": {"name": {"type": "str"}}},
    }

    params = {
        **messages(
            system("You are a helpful assistant outputting JSON."),
            user("Who won the world series in 2020?"),
        ),
        "response_format": response_format,
    }

    with pytest.raises(openai.BadRequestError):
        await querier.query(model, stream=False, **params)
