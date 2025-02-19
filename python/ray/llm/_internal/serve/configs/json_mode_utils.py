# TODO (genesu): remove dependency on jsonref.
import jsonref
from fastapi import status
from ray.llm._internal.serve.configs.openai_api_models import OpenAIHTTPException
import json
from jsonschema import Draft202012Validator, SchemaError, ValidationError
from typing import (
    Any,
    Dict,
    Optional,
    Union,
)

INVALID_JSON_REFERENCES_MSG = "Invalid JSON References. The schema provided has references ($refs) that were unable to be found."
INVALID_JSON_REFERENCES = "InvalidJsonReferences"
INVALID_RESPONSE_FORMAT_SCHEMA = "InvalidResponseFormatSchema"
INVALID_RESPONSE_FORMAT_SCHEMA_MSG = "The provided json schema was not valid."


# Enable strict mode by ensuring that the schema does not have any
# additional properties.
# https://github.com/python-jsonschema/jsonschema/issues/268#issuecomment-1828531763
_strict_metaschema = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "$id": "https://json-schema.org/draft/2020-12/strict",
    "$ref": "https://json-schema.org/draft/2020-12/schema",
    "unevaluatedProperties": False,
}
strict_validator = Draft202012Validator(_strict_metaschema)


def raise_invalid_response_format_schema(error_msg: str, e: Optional[Exception]):
    raise OpenAIHTTPException(
        message=INVALID_RESPONSE_FORMAT_SCHEMA_MSG + " Exception:\n" + error_msg,
        status_code=status.HTTP_400_BAD_REQUEST,
        type=INVALID_RESPONSE_FORMAT_SCHEMA,
    ) from e


def _dereference_json(schema: Optional[Union[str, Dict[str, Any]]]) -> Dict[str, Any]:
    """Remove $defs/definitions from json schema by dereferencing any references."""
    if schema is None:
        return {}
    if isinstance(schema, str):
        schema = json.loads(schema)
    try:
        schema = dict(
            jsonref.loads(
                json.dumps(schema),
                lazy_load=False,
                proxies=False,
            )
        )
    except jsonref.JsonRefError as e:
        # If the schema is invalid because references aren't able to be resolved,
        # we want to raise an error to the user.
        raise OpenAIHTTPException(
            message=INVALID_JSON_REFERENCES_MSG + ": " + str(e),
            status_code=status.HTTP_400_BAD_REQUEST,
            type=INVALID_JSON_REFERENCES,
        ) from e
    schema.pop("$defs", None)
    schema.pop("definitions", None)
    return schema


def try_load_json_schema(
    response_schema: Optional[Union[str, Dict[str, Any]]],
) -> Dict[str, Any]:
    """Try to load the json schema from the response format.

    - Attempt to validate the schema against Meta JSON Schema.
    - Dereference any definitions in the schema.

    Args:
        response_schema: The response format dictionary.

    """
    if response_schema is None:
        return {}
    try:
        if isinstance(response_schema, str):
            response_schema = json.loads(response_schema)
        elif isinstance(response_schema, dict):
            pass
        else:
            raise ValidationError(
                "Schema must be a string or a dict. "
                f"Got {type(response_schema)} instead."
            )
        strict_validator.validate(response_schema)
    except (ValidationError, SchemaError, json.JSONDecodeError) as e:
        error_msg = str(e)
        raise_invalid_response_format_schema(error_msg, e)

    response_schema = _dereference_json(response_schema)
    return response_schema
