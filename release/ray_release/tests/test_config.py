import sys
import yaml
import pytest

from ray_release.bazel import bazel_runfile
from ray_release.config import (
    read_and_validate_release_test_collection,
    Test,
    validate_cluster_compute,
    load_schema_file,
    parse_test_definition,
    validate_test,
)
from ray_release.exception import ReleaseTestConfigError

_TEST_COLLECTION_FILE = bazel_runfile("release/release_tests.yaml")

VALID_TEST = Test(
    **{
        "name": "validation_test",
        "group": "validation_group",
        "working_dir": "validation_dir",
        "python": "3.7",
        "frequency": "nightly",
        "team": "release",
        "cluster": {
            "cluster_env": "app_config.yaml",
            "cluster_compute": "tpl_cpu_small.yaml",
            "autosuspend_mins": 10,
        },
        "run": {
            "timeout": 100,
            "script": "python validate.py",
            "wait_for_nodes": {"num_nodes": 2, "timeout": 100},
            "type": "client",
        },
        "smoke_test": {"run": {"timeout": 20}, "frequency": "multi"},
        "alert": "default",
    }
)


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
            cluster_env: env.yaml
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
    assert gce_test["cluster"]["cluster_env"] == "env.yaml"
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


def test_schema_validation():
    test = VALID_TEST.copy()

    schema = load_schema_file()

    assert not validate_test(test, schema)

    # Remove some optional arguments
    del test["alert"]
    del test["python"]
    del test["run"]["wait_for_nodes"]
    del test["cluster"]["autosuspend_mins"]

    assert not validate_test(test, schema)

    # Add some faulty arguments

    # Faulty frequency
    invalid_test = test.copy()
    invalid_test["frequency"] = "invalid"

    assert validate_test(invalid_test, schema)

    # Faulty job type
    invalid_test = test.copy()
    invalid_test["run"]["type"] = "invalid"

    assert validate_test(invalid_test, schema)

    # Faulty file manager type
    invalid_test = test.copy()
    invalid_test["run"]["file_manager"] = "invalid"

    assert validate_test(invalid_test, schema)

    # Faulty smoke test
    invalid_test = test.copy()
    del invalid_test["smoke_test"]["frequency"]

    assert validate_test(invalid_test, schema)

    # Faulty Python version
    invalid_test = test.copy()
    invalid_test["python"] = "invalid"

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
    read_and_validate_release_test_collection(_TEST_COLLECTION_FILE)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
