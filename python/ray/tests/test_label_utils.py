import pytest
from ray._private.label_utils import (
    parse_node_labels_string,
    parse_node_labels_from_yaml_file,
    validate_node_labels,
    validate_label_key,
    validate_label_value,
    validate_label_selector_value,
)
import sys
import tempfile


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
            "Label value is not a key-value pair",
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
)
def test_validate_node_labels(labels_dict, expected_error, expected_message):
    if expected_error:
        with pytest.raises(expected_error) as e:
            validate_node_labels(labels_dict)
        assert expected_message in str(e.value)
    else:
        validate_node_labels(labels_dict)


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
)
def test_validate_label_value(value, should_raise, expected_message):
    if should_raise:
        with pytest.raises(ValueError) as e:
            validate_label_value(value)
        assert expected_message in str(e.value)
    else:
        validate_label_value(value)


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
