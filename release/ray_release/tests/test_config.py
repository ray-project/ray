import copy
import sys

import pytest
import yaml

from ray_release.config import (
    _substitute_variable,
    load_schema_file,
    parse_test_definition,
    read_and_validate_release_test_collection,
    validate_cluster_compute,
    validate_test,
)
from ray_release.exception import ReleaseTestConfigError
from ray_release.test import Test

_TEST_COLLECTION_FILES = [
    "release/release_tests.yaml",
    "release/release_data_tests.yaml",
    "release/release_multimodal_inference_benchmarks_tests.yaml",
    "release/ray_release/tests/test_collection_data.yaml",
]

VALID_TEST = {
    "name": "validation_test",
    "group": "validation_group",
    "working_dir": "validation_dir",
    "python": "3.9",
    "frequency": "nightly",
    "team": "release",
    "cluster": {
        "byod": {"type": "gpu"},
        "cluster_compute": "tpl_cpu_small.yaml",
        "autosuspend_mins": 10,
    },
    "run": {
        "timeout": 100,
        "script": "python validate.py",
        "wait_for_nodes": {"num_nodes": 2, "timeout": 100},
        "type": "client",
    },
    "smoke_test": {"run": {"timeout": 20}, "frequency": "nightly"},
    "alert": "default",
}


def test_parse_test_definition():
    """
    Unit test for the ray_release.config.parse_test_definition function. In particular,
    we check that the code correctly parse a test definition that have the 'variations'
    field.
    """
    test_definitions = yaml.safe_load(
        """
        - name: sample_test
          working_dir: sample_dir
          frequency: nightly
          team: sample
          cluster:
            byod:
              type: gpu
            cluster_compute: compute.yaml
          run:
            timeout: 100
            script: python script.py
          variations:
            - __suffix__: aws
            - __suffix__: gce
              cluster:
                cluster_compute: compute_gce.yaml
    """
    )
    # Check that parsing returns two tests, one for each variation (aws and gce). Check
    # that both tests are valid, and their fields are populated correctly
    tests = parse_test_definition(test_definitions)
    aws_test = tests[0]
    gce_test = tests[1]
    schema = load_schema_file()
    assert not validate_test(aws_test, schema)
    assert not validate_test(gce_test, schema)
    assert aws_test["name"] == "sample_test.aws"
    assert gce_test["cluster"]["cluster_compute"] == "compute_gce.yaml"
    assert gce_test["cluster"]["byod"]["type"] == "gpu"
    invalid_test_definition = test_definitions[0]
    # Intentionally make the test definition invalid by create an empty 'variations'
    # field. Check that the parser throws exception at runtime
    invalid_test_definition["variations"] = []
    with pytest.raises(ReleaseTestConfigError):
        parse_test_definition([invalid_test_definition])
    # Intentionally make the test definition invalid by making one 'variation' entry
    # missing the __suffix__ entry. Check that the parser throws exception at runtime
    invalid_test_definition["variations"] = [{"__suffix__": "aws"}, {}]
    with pytest.raises(ReleaseTestConfigError):
        parse_test_definition([invalid_test_definition])


def test_parse_test_definition_with_python_version():
    """
    Unit test for the ray_release.config.parse_test_definition function. In particular,
    we check that the code correctly parse a test definition that have the 'variations' & 'python'
    field.
    """
    test_definitions = yaml.safe_load(
        """
        - name: sample_test
          working_dir: sample_dir
          frequency: nightly
          team: sample
          python: "3.10"
          cluster:
            byod:
              type: gpu
            cluster_compute: compute.yaml
          run:
            timeout: 100
            script: python script.py
          variations:
            - __suffix__: aws
            - __suffix__: gce
              cluster:
                cluster_compute: compute_gce.yaml
    """
    )
    # Check that parsing returns two tests, one for each variation (aws and gce). Check
    # that both tests are valid, and their fields are populated correctly
    tests = parse_test_definition(test_definitions)
    aws_test = tests[0]
    gce_test = tests[1]
    schema = load_schema_file()
    assert not validate_test(aws_test, schema)
    assert not validate_test(gce_test, schema)
    assert aws_test["name"] == "sample_test.aws"
    assert gce_test["cluster"]["cluster_compute"] == "compute_gce.yaml"
    assert gce_test["cluster"]["byod"]["type"] == "gpu"
    invalid_test_definition = test_definitions[0]
    # Intentionally make the test definition invalid by create an empty 'variations'
    # field. Check that the parser throws exception at runtime
    invalid_test_definition["variations"] = []
    with pytest.raises(ReleaseTestConfigError):
        parse_test_definition([invalid_test_definition])
    # Intentionally make the test definition invalid by making one 'variation' entry
    # missing the __suffix__ entry. Check that the parser throws exception at runtime
    invalid_test_definition["variations"] = [{"__suffix__": "aws"}, {}]
    with pytest.raises(ReleaseTestConfigError):
        parse_test_definition([invalid_test_definition])


def test_parse_test_definition_with_defaults():
    test_definitions = yaml.safe_load(
        """
        - name: DEFAULTS
          working_dir: default_working_dir
        - name: sample_test_with_default_working_dir
          frequency: nightly
          team: sample
          cluster:
            byod:
              type: gpu
            cluster_compute: compute.yaml
          run:
            timeout: 100
            script: python script.py
        - name: sample_test_with_overridden_working_dir
          working_dir: overridden_working_dir
          frequency: nightly
          team: sample
          cluster:
            byod:
              type: gpu
            cluster_compute: compute.yaml
          run:
            timeout: 100
            script: python script.py
    """
    )
    test_with_default, test_with_override = parse_test_definition(test_definitions)
    schema = load_schema_file()
    assert not validate_test(test_with_default, schema)
    assert not validate_test(test_with_override, schema)
    assert test_with_default["working_dir"] == "default_working_dir"
    assert test_with_override["working_dir"] == "overridden_working_dir"


def test_parse_test_definition_with_matrix_and_variations_raises():
    # Matrix and variations are mutually exclusive.
    test_definitions = yaml.safe_load(
        """
        - name: test
          frequency: nightly
          team: team
          working_dir: sample_dir
          cluster:
            byod:
              type: gpu
            cluster_compute: "{{os}}.yaml"
          matrix:
            setup:
              os: [windows, linux]
          run:
            timeout: 100
            script: python script.py
          variations:
            - __suffix__: amd64
            - __suffix__: arm64
    """
    )
    with pytest.raises(ReleaseTestConfigError):
        parse_test_definition(test_definitions)


def test_parse_test_definition_with_matrix_and_adjustments():
    test_definitions = yaml.safe_load(
        """
        - name: "test-{{compute}}-{{arg}}"
          matrix:
            setup:
              compute: [fixed, autoscaling]
              arg: [0, 1]
            adjustments:
                - with:
                    # Only run arg 2 with fixed compute
                    compute: fixed
                    arg: 2
          frequency: nightly
          team: team
          working_dir: sample_dir
          cluster:
            byod:
              type: gpu
              runtime_env:
                - SCALING_MODE={{compute}}
            cluster_compute: "{{compute}}.yaml"
          run:
            timeout: 100
            script: python script.py --arg "{{arg}}"
    """
    )
    tests = parse_test_definition(test_definitions)
    schema = load_schema_file()

    assert len(tests) == 5  # 4 from matrix, 1 from adjustments
    assert not any(validate_test(test, schema) for test in tests)
    for i, (compute, arg) in enumerate(
        [
            ("fixed", 0),
            ("fixed", 1),
            ("autoscaling", 0),
            ("autoscaling", 1),
            ("fixed", 2),
        ]
    ):
        assert tests[i]["name"] == f"test-{compute}-{arg}"
        assert tests[i]["cluster"]["cluster_compute"] == f"{compute}.yaml"
        assert tests[i]["cluster"]["byod"]["runtime_env"] == [f"SCALING_MODE={compute}"]


class TestSubstituteVariable:
    def test_does_not_mutate_original(self):
        test_definition = {"name": "test-{{arg}}"}

        substituted = _substitute_variable(test_definition, "arg", "1")

        assert substituted is not test_definition
        assert test_definition == {"name": "test-{{arg}}"}

    def test_substitute_variable_in_string(self):
        test_definition = {"name": "test-{{arg}}"}

        substituted = _substitute_variable(test_definition, "arg", "1")

        assert substituted == {"name": "test-1"}

    def test_substitute_variable_in_list(self):
        test_definition = {"items": ["item-{{arg}}"]}

        substituted = _substitute_variable(test_definition, "arg", "1")

        assert substituted == {"items": ["item-1"]}

    def test_substitute_variable_in_dict(self):
        test_definition = {"outer": {"inner": "item-{{arg}}"}}

        substituted = _substitute_variable(test_definition, "arg", "1")

        assert substituted == {"outer": {"inner": "item-1"}}


def test_schema_validation():
    test = VALID_TEST.copy()

    schema = load_schema_file()

    assert not validate_test(Test(**test), schema)

    # Remove some optional arguments
    del test["alert"]
    del test["python"]
    del test["run"]["wait_for_nodes"]
    del test["cluster"]["autosuspend_mins"]

    assert not validate_test(Test(**test), schema)

    # Add some faulty arguments

    # Faulty frequency
    invalid_test = Test(**copy.deepcopy(VALID_TEST))
    invalid_test["frequency"] = "invalid"

    assert validate_test(invalid_test, schema)

    # Faulty job type
    invalid_test = Test(**copy.deepcopy(VALID_TEST))
    invalid_test["run"]["type"] = "invalid"

    assert validate_test(invalid_test, schema)

    # Faulty file manager type
    invalid_test = Test(**copy.deepcopy(VALID_TEST))
    invalid_test["run"]["file_manager"] = "invalid"

    assert validate_test(invalid_test, schema)

    # Faulty smoke test

    invalid_test = Test(**copy.deepcopy(VALID_TEST))
    del invalid_test["smoke_test"]["frequency"]

    assert validate_test(invalid_test, schema)

    # Faulty Python version
    invalid_test = Test(**copy.deepcopy(VALID_TEST))
    invalid_test["python"] = "invalid"

    assert validate_test(invalid_test, schema)

    # Faulty BYOD type
    invalid_test = Test(**copy.deepcopy(VALID_TEST))
    invalid_test["cluster"]["byod"]["type"] = "invalid"
    assert validate_test(invalid_test, schema)

    # Faulty BYOD and Python version match
    invalid_test = Test(**copy.deepcopy(VALID_TEST))
    invalid_test["cluster"]["byod"]["type"] = "gpu"
    invalid_test["python"] = "3.11"
    assert validate_test(invalid_test, schema)


def test_compute_config_invalid_ebs():
    compute_config = {
        "aws": {
            "BlockDeviceMappings": [
                {
                    "DeviceName": "/dev/sda1",
                    "Ebs": {
                        "VolumeSize": 1000,
                    },
                }
            ]
        }
    }
    assert validate_cluster_compute(compute_config)

    compute_config["aws"]["BlockDeviceMappings"][0]["Ebs"][
        "DeleteOnTermination"
    ] = False

    assert validate_cluster_compute(compute_config)

    compute_config["aws"]["BlockDeviceMappings"][0]["Ebs"]["DeleteOnTermination"] = True

    assert not validate_cluster_compute(compute_config)

    compute_config["head_node_type"] = {}
    compute_config["head_node_type"]["aws_advanced_configurations"] = {
        "BlockDeviceMappings": [
            {
                "DeviceName": "/dev/sda1",
                "Ebs": {
                    "VolumeSize": 1000,
                },
            }
        ]
    }

    assert validate_cluster_compute(compute_config)

    compute_config["head_node_type"]["aws_advanced_configurations"][
        "BlockDeviceMappings"
    ][0]["Ebs"]["DeleteOnTermination"] = False

    assert validate_cluster_compute(compute_config)

    compute_config["head_node_type"]["aws_advanced_configurations"][
        "BlockDeviceMappings"
    ][0]["Ebs"]["DeleteOnTermination"] = True

    assert not validate_cluster_compute(compute_config)

    compute_config["worker_node_types"] = [{}]
    compute_config["worker_node_types"][0]["aws_advanced_configurations"] = {
        "BlockDeviceMappings": [
            {
                "DeviceName": "/dev/sda1",
                "Ebs": {
                    "VolumeSize": 1000,
                },
            }
        ]
    }

    assert validate_cluster_compute(compute_config)

    compute_config["worker_node_types"][0]["aws_advanced_configurations"][
        "BlockDeviceMappings"
    ][0]["Ebs"]["DeleteOnTermination"] = False

    assert validate_cluster_compute(compute_config)

    compute_config["worker_node_types"][0]["aws_advanced_configurations"][
        "BlockDeviceMappings"
    ][0]["Ebs"]["DeleteOnTermination"] = True

    assert not validate_cluster_compute(compute_config)


def test_load_and_validate_test_collection_file():
    tests = read_and_validate_release_test_collection(_TEST_COLLECTION_FILES)
    assert [test for test in tests if test.get_name() == "test_name"]


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
