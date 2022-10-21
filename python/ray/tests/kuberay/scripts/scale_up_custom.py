import os

import pytest

import ray

import time


def main():
    """Submits custom resource request.

    Also, validates runtime env data submitted with the Ray Job that executes
    this script.
    """
    # The next two lines validate the runtime env in which this code runs.
    # (See the function ray_job_submit() in tests/kuberay/utils.py)
    assert pytest.__version__ == "6.0.0"
    assert os.getenv("key_foo") == "value_bar"

    # Workers and head are annotated as having 5 "Custom2" capacity each,
    # so this should trigger upscaling of two workers.
    # (One of the bundles will be "placed" on the head.)
    ray.autoscaler.sdk.request_resources(
        bundles=[{"Custom2": 3}, {"Custom2": 3}, {"Custom2": 3}]
    )

    while (
        ray.cluster_resources().get("Custom2", 0) < 3
        and ray.cluster_resources().get("Custom2", 0) < 6
    ):
        time.sleep(0.1)

    # Output something to validate the job logs.
    print("Submitted custom scale request!")


if __name__ == "__main__":
    ray.init("auto")
    main()
