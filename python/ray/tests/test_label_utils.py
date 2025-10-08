import json
import os
import sys
import tempfile
from contextlib import contextmanager
from typing import ContextManager, Dict, Optional, Union

import pytest

from ray._private.label_utils import (
    parse_node_labels_from_yaml_file,
    parse_node_labels_json,
    parse_node_labels_string,
    validate_fallback_strategy,
    validate_label_key,
    validate_label_selector,
    validate_label_selector_value,
    validate_label_value,
    validate_node_label_syntax,
    validate_node_labels,
)


@pytest.mark.parametrize(
    "labels_string, should_raise, expected_result, expected_error_msg",
    [
        # Valid - Empty label string
        ("", False, {}, None),
        # Valid - Empty label value
        ("region=", False, {"region": ""}, None),
        # Valid - multiple label key-value pairs
        (
            "ray.io/accelerator-type=A100,region=us-west4",
            False,
            {"ray.io/accelerator-type": "A100", "region": "us-west4"},
            None,
        ),
        # Invalid - label is not a key-value pair
        (
            "ray.io/accelerator-type=type=A100",
            True,
            None,
            "Label string is not a key-value pair",
        ),
    ],
    ids=["empty-string", "empty-value", "multi-kv", "not-kv"],
)
def test_parse_node_labels_from_string(
    labels_string, should_raise, expected_result, expected_error_msg
):
    if should_raise:
        with pytest.raises(ValueError) as e:
            parse_node_labels_string(labels_string)
        assert expected_error_msg in str(e.value)
    else:
        labels_dict = parse_node_labels_string(labels_string)
        assert labels_dict == expected_result


def test_parse_node_labels_from_json():
    # Empty label argument passed
    labels_json = "{}"
    labels_dict = parse_node_labels_json(labels_json)
    assert labels_dict == {}

    # Invalid label prefix - starts with ray.io
    labels_json = '{"ray.io/accelerator-type": "A100"}'
    with pytest.raises(ValueError) as e:
        parse_node_labels_json(labels_json)
    assert "This is reserved for Ray defined labels" in str(e)

    # Valid label key with empty value
    labels_json = '{"region":""}'
    labels_dict = parse_node_labels_json(labels_json)
    assert labels_dict == {"region": ""}

    # Multiple valid label keys and values
    labels_json = '{"accelerator-type":"A100", "region":"us-west4"}'
    labels_dict = parse_node_labels_json(labels_json)
    assert labels_dict == {"accelerator-type": "A100", "region": "us-west4"}

    # Invalid label key - json.loads should fail
    labels_json = '{500:"A100"}'
    with pytest.raises(json.decoder.JSONDecodeError):
        parse_node_labels_json(labels_json)

    # Invalid label value - must be string type
    labels_json = '{"accelerator-type":500}'
    with pytest.raises(ValueError) as e:
        parse_node_labels_json(labels_json)
    assert 'The value of the "accelerator-type" is not string type' in str(e)


@contextmanager
def _tempfile(content: str) -> ContextManager[str]:
    """Yields a temporary file containing the provided content.

    NOTE: we cannot use the built-in NamedTemporaryFile context manager because it
    causes test failures on Windows due to holding the file descriptor open.
    """
    f = tempfile.NamedTemporaryFile(mode="w+", delete=False)
    try:
        f.write(content)
        f.flush()
        f.close()
        yield f.name
    finally:
        os.unlink(f.name)


def test_parse_node_labels_from_missing_yaml_file():
    with pytest.raises(FileNotFoundError):
        parse_node_labels_from_yaml_file("missing-file.yaml")


@pytest.mark.parametrize(
    "content, expected_output, exception_match",
    [
        # Empty/invalid YAML file.
        ("", ValueError, "is not a key-value pair map"),
        # Invalid label key (not a string).
        ('{100: "A100"}', ValueError, "The key is not string type."),
        # Invalid label value (not a string).
        ('{"gpu": 100}', ValueError, 'The value of "gpu" is not string type'),
        # Valid file with empty label value.
        ('"ray.io/accelerator-type": ""', {"ray.io/accelerator-type": ""}, None),
        # Multiple valid label keys and values.
        (
            '"ray.io/accelerator-type": "A100"\n"region": "us"\n"market-type": "spot"',
            {
                "ray.io/accelerator-type": "A100",
                "region": "us",
                "market-type": "spot",
            },
            None,
        ),
    ],
    ids=["empty", "invalid-key", "invalid-value", "empty-value", "multi-kv"],
)
def test_parse_node_labels_from_yaml_file(
    content: str,
    expected_output: Union[Dict, Exception],
    exception_match: Optional[str],
):
    with _tempfile(content) as p:
        if not isinstance(expected_output, dict):
            with pytest.raises(expected_output, match=exception_match):
                parse_node_labels_from_yaml_file(p)
        else:
            assert parse_node_labels_from_yaml_file(p) == expected_output


@pytest.mark.parametrize(
    "labels_dict, expected_error, expected_message",
    [
        (  # Invalid key prefix syntax
            {"!invalidPrefix/accelerator-type": "A100"},
            ValueError,
            "Invalid label key prefix",
        ),
        (  # Invalid key name syntax
            {"!!accelerator-type?": "A100"},
            ValueError,
            "Invalid label key name",
        ),
        (  # Invalid value syntax
            {"accelerator-type": "??"},
            ValueError,
            "Invalid label key value",
        ),
        (  # Valid label
            {"accelerator-type": "A100"},
            None,
            None,
        ),
    ],
    ids=["invalid-key-prefix", "invalid-key", "invalid-value", "valid"],
)
def test_validate_node_label_syntax(labels_dict, expected_error, expected_message):
    if expected_error:
        with pytest.raises(expected_error) as e:
            validate_node_label_syntax(labels_dict)
        assert expected_message in str(e.value)
    else:
        validate_node_label_syntax(labels_dict)


@pytest.mark.parametrize(
    "key, expected_error",
    [
        ("ray.io/accelerator-type", None),
        ("region", None),
        ("-region", "Invalid label key name"),
        ("region-", "Invalid label key name"),
        ("ray!.io/accelerator-type", "Invalid label key prefix"),
        ("a" * 64, "Invalid label key name"),
    ],
    ids=[
        "valid1",
        "valid2",
        "invalid-prefix",
        "invalid-suffix",
        "invalid-noteq",
        "too-long",
    ],
)
def test_validate_label_key(key, expected_error):
    error_msg = validate_label_key(key)
    if expected_error:
        assert expected_error in error_msg
    else:
        assert error_msg is None


@pytest.mark.parametrize(
    "value, should_raise, expected_message",
    [
        ("", False, None),
        ("valid-value", False, None),
        ("_underscore-start", True, "Invalid label key value"),
        ("dash-end-", True, "Invalid label key value"),
        ("@invalid", True, "Invalid label key value"),
        ("a" * 64, True, "Invalid label key value"),
    ],
    ids=["empty", "valid", "invalid-prefix", "invalid-suffix", "bad-char", "too-long"],
)
def test_validate_label_value(value, should_raise, expected_message):
    if should_raise:
        with pytest.raises(ValueError) as e:
            validate_label_value(value)
        assert expected_message in str(e.value)
    else:
        validate_label_value(value)


@pytest.mark.parametrize(
    "label_selector, expected_error",
    [
        (None, None),  # Valid: No input provided
        ({"region": "us-west4"}, None),  # Valid label key and value
        ({"ray.io/accelerator-type": "A100"}, None),  # Valid label key and value
        ({"": "valid-value"}, "Invalid label key name"),  # Invalid label key (empty)
        (
            {"!-invalidkey": "valid-value"},
            "Invalid label key name",
        ),  # Invalid label key syntax
        (
            {"valid-key": "a" * 64},
            "Invalid label selector value",
        ),  # Invalid label value syntax
    ],
)
def test_validate_label_selector(label_selector, expected_error):
    result = validate_label_selector(label_selector)
    if expected_error:
        assert expected_error in result
    else:
        assert result is None


@pytest.mark.parametrize(
    "selector, expected_error",
    [
        ("spot", None),
        ("!GPU", None),
        ("in(A123, B456, C789)", None),
        ("!in(spot, on-demand)", None),
        ("valid-value", None),
        ("-spot", "Invalid label selector value"),
        ("spot_", "Invalid label selector value"),
        ("in()", "Invalid label selector value"),
        ("in(spot,", "Invalid label selector value"),
        ("in(H100, TPU!GPU)", "Invalid label selector value"),
        ("!!!in(H100, TPU)", "Invalid label selector value"),
        ("a" * 64, "Invalid label selector value"),
    ],
    ids=[
        "spot",
        "no-gpu",
        "in",
        "not-in",
        "valid",
        "invalid-prefix",
        "invalid-suffix",
        "invalid-in",
        "unfinished-in",
        "invalid-noteq",
        "triple-noteq",
        "too-long",
    ],
)
def test_validate_label_selector_value(selector, expected_error):
    error_msg = validate_label_selector_value(selector)
    if expected_error:
        assert expected_error in error_msg
    else:
        assert error_msg is None


def test_validate_node_labels():
    # Custom label starts with ray.io prefix
    labels_dict = {"ray.io/accelerator-type": "A100"}
    with pytest.raises(ValueError) as e:
        validate_node_labels(labels_dict)
    assert "This is reserved for Ray defined labels." in str(e)


@pytest.mark.parametrize(
    "fallback_strategy, expected_error",
    [
        (None, None),  # No fallback_strategy specified.
        ([], None),  # fallback_strategy passed an empty list.
        (
            [
                {"label_selector": {"valid-key": "valid-value"}, "memory": "500m"},
            ],
            "Unsupported option found: 'memory'. Only ['label_selector'] is currently supported.",
        ),  # fallback_strategy contains unsupported option.
        (
            [
                {},
            ],
            "Empty dictionary found in `fallback_strategy`.",
        ),  # fallback_strategy contains empty dictionary.
        (
            [{"label_selector": {"ray.io/availability-region": "us-west4"}}],
            None,
        ),  # fallback_strategy contains one selector.
        (
            [
                {"label_selector": {"ray.io/availability-zone": "us-central1-a"}},
                {"label_selector": {"ray.io/accelerator-type": "A100"}},
            ],
            None,
        ),  # fallback_strategy contains multiple valid selectors.
        (
            [
                {"label_selector": {"valid-key": "valid-value"}},
                {"label_selector": {"-!!invalid-key": "value"}},
            ],
            "Invalid label key name",
        ),  # fallback_strategy contains selector with invalid key.
        (
            [
                {"label_selector": {"valid-key": "valid-value"}},
                {"label_selector": {"key": "-invalid-value!!"}},
            ],
            "Invalid label selector value",
        ),  # fallback_strategy contains selector with invalid value.
    ],
    ids=[
        "none",
        "empty-list",
        "unsupported-fallback-option",
        "fallback-specified-with-empty-dict",
        "single-valid-label-selector",
        "multiple-valid-label-selector",
        "invalid-label-selector-key",
        "invalid-label-selector-value",
    ],
)
def test_validate_fallback_strategy(fallback_strategy, expected_error):
    """Tests the validation logic for the fallback_strategy remote option."""
    result = validate_fallback_strategy(fallback_strategy)
    if expected_error:
        assert expected_error in result
    else:
        assert result is None


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", "-vv", __file__]))
