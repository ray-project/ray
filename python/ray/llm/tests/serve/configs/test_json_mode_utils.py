import pytest
from ray.llm._internal.serve.configs.json_mode_utils import (
    JSONSchemaValidator,
    INVALID_JSON_REFERENCES,
    INVALID_RESPONSE_FORMAT_SCHEMA,
)
from ray.llm._internal.serve.configs.openai_api_models import OpenAIHTTPException


def test_singleton_pattern():
    """Test that JSONSchemaValidator follows singleton pattern."""
    validator1 = JSONSchemaValidator()
    validator2 = JSONSchemaValidator()
    assert validator1 is validator2
    assert validator1._validator is validator2._validator


def test_validator_initialization():
    """Test that validator is initialized correctly."""
    validator = JSONSchemaValidator()
    assert validator._validator is not None
    # Test that accessing property works
    assert validator.strict_validator is validator._validator


def test_validate_valid_schema():
    """Test validation of a valid JSON schema."""
    validator = JSONSchemaValidator()
    valid_schema = {
        "type": "object",
        "properties": {"name": {"type": "string"}, "age": {"type": "integer"}},
        "required": ["name"],
    }
    # Should not raise any exceptions
    result = validator.try_load_json_schema(valid_schema)
    assert result == valid_schema


def test_validate_invalid_schema():
    """Test validation of an invalid JSON schema."""
    validator = JSONSchemaValidator()
    invalid_schema = {"type": "invalid_type", "properties": "not_an_object"}
    with pytest.raises(OpenAIHTTPException) as exc_info:
        validator.try_load_json_schema(invalid_schema)
    assert exc_info.value.type == INVALID_RESPONSE_FORMAT_SCHEMA


def test_dereference_json():
    """Test JSON dereferencing functionality."""
    validator = JSONSchemaValidator()
    schema_with_refs = {
        "$defs": {
            "address": {"type": "object", "properties": {"street": {"type": "string"}}}
        },
        "type": "object",
        "properties": {"home": {"$ref": "#/$defs/address"}},
    }
    result = validator._dereference_json(schema_with_refs)
    # Check that $defs was removed
    assert "$defs" not in result
    # Check that reference was resolved
    assert result["properties"]["home"]["type"] == "object"
    assert result["properties"]["home"]["properties"]["street"]["type"] == "string"


def test_invalid_references():
    """Test handling of invalid JSON references."""
    validator = JSONSchemaValidator()
    schema_with_bad_ref = {
        "type": "object",
        "properties": {"bad": {"$ref": "#/nonexistent"}},
    }
    with pytest.raises(OpenAIHTTPException) as exc_info:
        validator._dereference_json(schema_with_bad_ref)
    assert exc_info.value.type == INVALID_JSON_REFERENCES


def test_none_schema():
    """Test handling of None schema."""
    validator = JSONSchemaValidator()
    result = validator.try_load_json_schema(None)
    assert result == {}


def test_string_schema():
    """Test handling of schema passed as JSON string."""
    validator = JSONSchemaValidator()
    schema_str = '{"type": "object", "properties": {"name": {"type": "string"}}}'
    result = validator.try_load_json_schema(schema_str)
    assert isinstance(result, dict)
    assert result["type"] == "object"
