import pytest

from ray.autoscaler._private.gcp.node import GCPCompute

_PROJECT_NAME = "project-one"
_AZ = "us-west1-b"


@pytest.mark.parametrize(
    "test_case",
    [
        ("n1-standard-4", f"zones/{_AZ}/machineTypes/n1-standard-4"),
        (
            f"zones/{_AZ}/machineTypes/n1-standard-4",
            f"zones/{_AZ}/machineTypes/n1-standard-4",
        ),
    ],
)
def test_convert_resources_to_urls_machine(test_case):
    gcp_compute = GCPCompute(None, _PROJECT_NAME, _AZ, "cluster_name")
    base_machine, result_machine = test_case
    modified_config = gcp_compute._convert_resources_to_urls(
        {"machineType": base_machine}
    )
    assert modified_config["machineType"] == result_machine


@pytest.mark.parametrize(
    "test_case",
    [
        (
            "nvidia-tesla-k80",
            f"projects/{_PROJECT_NAME}/zones/{_AZ}/acceleratorTypes/nvidia-tesla-k80",
        ),
        (
            f"projects/{_PROJECT_NAME}/zones/{_AZ}/acceleratorTypes/nvidia-tesla-k80",
            f"projects/{_PROJECT_NAME}/zones/{_AZ}/acceleratorTypes/nvidia-tesla-k80",
        ),
    ],
)
def test_convert_resources_to_urls_accelerators(test_case):
    gcp_compute = GCPCompute(None, _PROJECT_NAME, _AZ, "cluster_name")
    base_accel, result_accel = test_case

    base_config = {
        "machineType": "n1-standard-4",
        "guestAccelerators": [{"acceleratorCount": 1, "acceleratorType": base_accel}],
    }
    modified_config = gcp_compute._convert_resources_to_urls(base_config)

    assert modified_config["guestAccelerators"][0]["acceleratorType"] == result_accel


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
