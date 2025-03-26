# coding: utf-8
"""
Unit/Integration Testing for python/ray/_private/utils.py

This currently expects to work for minimal installs.
"""

import click
import pytest
import logging
from ray._private.utils import (
    parse_pg_formatted_resources_to_original,
    try_import_each_module,
    get_current_node_cpu_model_name,
    parse_node_labels_string,
    parse_node_labels_json,
    validate_node_labels,
)
from ray.autoscaler._private.cli_logger import cf, cli_logger
from unittest.mock import patch, mock_open
import sys

logger = logging.getLogger(__name__)


def test_try_import_each_module():
    mock_sys_modules = sys.modules.copy()
    modules_to_import = ["json", "logging", "re"]
    fake_module = "fake_import_does_not_exist"

    for lib in modules_to_import:
        if lib in mock_sys_modules:
            del mock_sys_modules[lib]

    with patch("ray._private.utils.logger.exception") as mocked_log_exception:
        with patch.dict(sys.modules, mock_sys_modules):
            patched_sys_modules = sys.modules

            try_import_each_module(
                modules_to_import[:1] + [fake_module] + modules_to_import[1:]
            )

            # Verify modules are imported.
            for lib in modules_to_import:
                assert (
                    lib in patched_sys_modules
                ), f"lib {lib} not in {patched_sys_modules}"

            # Verify import error is printed.
            found = False
            for args, _ in mocked_log_exception.call_args_list:
                found = any(fake_module in arg for arg in args)
                if found:
                    break

            assert found, (
                "Did not find print call with import "
                f"error {mocked_log_exception.call_args_list}"
            )


def test_parse_pg_formatted_resources():
    out = parse_pg_formatted_resources_to_original(
        {"CPU_group_e765be422c439de2cd263c5d9d1701000000": 1, "memory": 100}
    )
    assert out == {"CPU": 1, "memory": 100}

    out = parse_pg_formatted_resources_to_original(
        {
            "memory_group_4da1c24ac25bec85bc817b258b5201000000": 100.0,
            "memory_group_0_4da1c24ac25bec85bc817b258b5201000000": 100.0,
            "CPU_group_0_4da1c24ac25bec85bc817b258b5201000000": 1.0,
            "CPU_group_4da1c24ac25bec85bc817b258b5201000000": 1.0,
        }
    )
    assert out == {"CPU": 1, "memory": 100}


def test_parse_node_labels_from_string():
    # Empty label argument passed
    labels_string = ""
    labels_dict = parse_node_labels_string(labels_string, cli_logger, cf)
    assert labels_dict == {}

    # Valid label key with empty value
    labels_string = "region="
    labels_dict = parse_node_labels_string(labels_string, cli_logger, cf)
    assert labels_dict == {"region": ""}

    # Multiple valid label keys and values
    labels_string = "ray.io/accelerator-type=A100,region=us-west4"
    labels_dict = parse_node_labels_string(labels_string, cli_logger, cf)
    assert labels_dict == {"ray.io/accelerator-type": "A100", "region": "us-west4"}

    # Invalid label
    labels_string = "ray.io/accelerator-type=type=A100"
    with pytest.raises(click.exceptions.ClickException) as e:
        parse_node_labels_string(labels_string, cli_logger, cf)
    assert "is not a valid string of key-value pairs" in str(e)


def test_parse_node_labels_from_json():
    # Empty/invalid json
    labels_json = ""
    labels_dict = parse_node_labels_json(labels_json, cli_logger, cf)
    assert labels_dict == {}

    # Valid label key with empty value
    labels_json = '{"ray.io/accelerator-type": ""}'
    labels_dict = parse_node_labels_json(labels_json, cli_logger, cf)
    assert labels_dict == {"ray.io/accelerator-type": ""}

    # Multiple valid label keys and values
    labels_json = (
        '{"ray.io/accelerator-type": "A100", "region": "us", "market-type": "spot"}'
    )
    labels_dict = parse_node_labels_json(labels_json, cli_logger, cf)
    assert labels_dict == {
        "ray.io/accelerator-type": "A100",
        "region": "us",
        "market-type": "spot",
    }

    # Non-string label key
    labels_json = '{100: "A100"}'
    with pytest.raises(click.exceptions.ClickException) as e:
        parse_node_labels_json(labels_json, cli_logger, cf)
    assert "is not a valid JSON string" in str(e)

    # Non-string label value
    labels_json = '{"ray.io/accelerator-type": 5}'
    with pytest.raises(click.exceptions.ClickException) as e:
        parse_node_labels_json(labels_json, cli_logger, cf)
    assert "is not a valid JSON string" in str(e)


def test_validate_node_labels():
    # Custom label starts with ray.io prefix
    labels_dict = {"ray.io/accelerator-type": "A100"}
    with pytest.raises(ValueError) as e:
        validate_node_labels(labels_dict)
    assert "This is reserved for Ray defined labels." in str(e)

    # Invalid key prefix syntax
    labels_dict = {"invalidPrefix/accelerator-type": "A100"}
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


@pytest.mark.skipif(
    not sys.platform.startswith("linux"), reason="Doesn't support non-linux"
)
def test_get_current_node_cpu_model_name():
    with patch(
        "builtins.open", mock_open(read_data="processor: 0\nmodel name: Intel Xeon")
    ):
        assert get_current_node_cpu_model_name() == "Intel Xeon"


if __name__ == "__main__":
    import os

    # Skip test_basic_2_client_mode for now- the test suite is breaking.
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
