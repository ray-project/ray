import os
import sys
import pytest

from ray_release.config import (
    read_and_validate_release_test_collection,
    Test,
    validate_release_test_collection,
    validate_cluster_compute,
)
from ray_release.exception import ReleaseTestConfigError

TEST_COLLECTION_FILE = os.path.join(
    os.path.dirname(__file__), "..", "..", "release_tests.yaml"
)


VALID_TEST = Test(
    **{
        "name": "validation_test",
        "group": "validation_group",
        "working_dir": "validation_dir",
        "legacy": {
            "test_name": "validation_test",
            "test_suite": "validation_suite",
        },
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


def test_schema_validation():
    test = VALID_TEST.copy()

    validate_release_test_collection([test])

    # Remove some optional arguments
    del test["alert"]
    del test["python"]
    del test["run"]["wait_for_nodes"]
    del test["cluster"]["autosuspend_mins"]

    validate_release_test_collection([test])

    # Add some faulty arguments

    # Faulty frequency
    invalid_test = test.copy()
    invalid_test["frequency"] = "invalid"

    with pytest.raises(ReleaseTestConfigError):
        validate_release_test_collection([invalid_test])

    # Faulty job type
    invalid_test = test.copy()
    invalid_test["run"]["type"] = "invalid"
    with pytest.raises(ReleaseTestConfigError):
        validate_release_test_collection([invalid_test])

    # Faulty file manager type
    invalid_test = test.copy()
    invalid_test["run"]["file_manager"] = "invalid"
    with pytest.raises(ReleaseTestConfigError):
        validate_release_test_collection([invalid_test])

    # Faulty smoke test
    invalid_test = test.copy()
    del invalid_test["smoke_test"]["frequency"]
    with pytest.raises(ReleaseTestConfigError):
        validate_release_test_collection([invalid_test])

    # Faulty Python version
    invalid_test = test.copy()
    invalid_test["python"] = "invalid"
    with pytest.raises(ReleaseTestConfigError):
        validate_release_test_collection([invalid_test])


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


def test_load_and_validate_test_collection_file():
    read_and_validate_release_test_collection(TEST_COLLECTION_FILE)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
