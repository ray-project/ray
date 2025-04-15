import json
import pytest
from ray._private.label_utils import (
    parse_node_labels_json,
    parse_node_labels_string,
    parse_node_labels_from_yaml_file,
    validate_node_label_syntax,
    validate_label_selector_value,
)
import sys
import tempfile


def test_parse_node_labels_from_json():
    # Empty label argument passed
    labels_json = "{}"
    labels_dict = parse_node_labels_json(labels_json)
    assert labels_dict == {}

    # Valid label key with empty value
    labels_json = '{"region":""}'
    labels_dict = parse_node_labels_json(labels_json)
    assert labels_dict == {"region": ""}

    # Multiple valid label keys and values
    labels_json = '{"ray.io/accelerator-type":"A100", "region":"us-west4"}'
    labels_dict = parse_node_labels_json(labels_json)
    assert labels_dict == {"ray.io/accelerator-type": "A100", "region": "us-west4"}

    # Invalid label key - json.loads should fail
    labels_json = '{500:"A100"}'
    with pytest.raises(json.decoder.JSONDecodeError):
        parse_node_labels_json(labels_json)

    # Invalid label value - must be string type
    labels_json = '{"ray.io/accelerator-type":500}'
    with pytest.raises(ValueError) as e:
        parse_node_labels_json(labels_json)
    assert 'The value of the "ray.io/accelerator-type" is not string type' in str(e)


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


def test_parse_node_labels_from_yaml_file():
    # Empty/invalid yaml
    with tempfile.NamedTemporaryFile(mode="w+", delete=True) as test_file:
        test_file.write("")
        test_file.flush()  # Ensure data is written
        with pytest.raises(ValueError) as e:
            parse_node_labels_from_yaml_file(test_file.name)
        assert "The format after deserialization is not a key-value pair map" in str(e)

    # With non-existent yaml file
    with pytest.raises(FileNotFoundError):
        parse_node_labels_from_yaml_file("missing-file.yaml")

    # Valid label key with empty value
    with tempfile.NamedTemporaryFile(mode="w+", delete=True) as test_file:
        test_file.write('"ray.io/accelerator-type": ""')
        test_file.flush()  # Ensure data is written
        labels_dict = parse_node_labels_from_yaml_file(test_file.name)
    assert labels_dict == {"ray.io/accelerator-type": ""}

    # Multiple valid label keys and values
    with tempfile.NamedTemporaryFile(mode="w+", delete=True) as test_file:
        test_file.write(
            '"ray.io/accelerator-type": "A100"\n"region": "us"\n"market-type": "spot"'
        )
        test_file.flush()  # Ensure data is written
        labels_dict = parse_node_labels_from_yaml_file(test_file.name)
    assert labels_dict == {
        "ray.io/accelerator-type": "A100",
        "region": "us",
        "market-type": "spot",
    }

    # Non-string label key
    with tempfile.NamedTemporaryFile(mode="w+", delete=True) as test_file:
        test_file.write('{100: "A100"}')
        test_file.flush()  # Ensure data is written
        with pytest.raises(ValueError) as e:
            parse_node_labels_from_yaml_file(test_file.name)
    assert "The key is not string type." in str(e)

    # Non-string label value
    with tempfile.NamedTemporaryFile(mode="w+", delete=True) as test_file:
        test_file.write('{"gpu": 100}')
        test_file.flush()  # Ensure data is written
        with pytest.raises(ValueError) as e:
            parse_node_labels_from_yaml_file(test_file.name)
    assert 'The value of "gpu" is not string type' in str(e)


def test_validate_node_label_syntax():
    # Invalid key prefix syntax
    labels_dict = {"!invalidPrefix/accelerator-type": "A100"}
    with pytest.raises(ValueError) as e:
        validate_node_label_syntax(labels_dict)
    assert "Invalid label key prefix" in str(e)

    # Invalid key name syntax
    labels_dict = {"!!accelerator-type?": "A100"}
    with pytest.raises(ValueError) as e:
        validate_node_label_syntax(labels_dict)
    assert "Invalid label key name" in str(e)

    # Invalid key value syntax
    labels_dict = {"accelerator-type": "??"}
    with pytest.raises(ValueError) as e:
        validate_node_label_syntax(labels_dict)
    assert "Invalid label key value" in str(e)

    # Valid node label
    labels_dict = {"accelerator-type": "A100"}
    validate_node_label_syntax(labels_dict)


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
)
def test_validate_label_selector_value(selector, expected_error):
    error_msg = validate_label_selector_value(selector)
    if expected_error:
        assert expected_error in error_msg
    else:
        assert error_msg is None


if __name__ == "__main__":
    import os

    # Skip test_basic_2_client_mode for now- the test suite is breaking.
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
