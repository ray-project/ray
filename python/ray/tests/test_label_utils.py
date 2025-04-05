import pytest
from ray._private.label_utils import (
    parse_node_labels_string,
    parse_node_labels_from_yaml_file,
    validate_node_labels,
)
import sys
import tempfile


def test_parse_node_labels_from_string():
    # Empty label argument passed
    labels_string = ""
    labels_dict = parse_node_labels_string(labels_string)
    assert labels_dict == {}

    # Valid label key with empty value
    labels_string = "region="
    labels_dict = parse_node_labels_string(labels_string)
    assert labels_dict == {"region": ""}

    # Multiple valid label keys and values
    labels_string = "ray.io/accelerator-type=A100,region=us-west4"
    labels_dict = parse_node_labels_string(labels_string)
    assert labels_dict == {"ray.io/accelerator-type": "A100", "region": "us-west4"}

    # Invalid label
    labels_string = "ray.io/accelerator-type=type=A100"
    with pytest.raises(ValueError) as e:
        parse_node_labels_string(labels_string)
    assert "Label value is not a key-value pair" in str(e)


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


def test_validate_node_labels():
    # Invalid key prefix syntax
    labels_dict = {"!invalidPrefix/accelerator-type": "A100"}
    with pytest.raises(ValueError) as e:
        validate_node_labels(labels_dict)
    assert "Invalid label key prefix" in str(e)

    # Invalid key name syntax
    labels_dict = {"!!accelerator-type?": "A100"}
    with pytest.raises(ValueError) as e:
        validate_node_labels(labels_dict)
    assert "Invalid label key name" in str(e)

    # Invalid key value syntax
    labels_dict = {"accelerator-type": "??"}
    with pytest.raises(ValueError) as e:
        validate_node_labels(labels_dict)
    assert "Invalid label key value" in str(e)

    # Valid node label
    labels_dict = {"accelerator-type": "A100"}
    validate_node_labels(labels_dict)


if __name__ == "__main__":
    import os

    # Skip test_basic_2_client_mode for now- the test suite is breaking.
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
