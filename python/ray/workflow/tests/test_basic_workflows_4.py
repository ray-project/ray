"""Basic tests isolated from other tests for shared fixtures."""
import os
import pytest

import ray
from ray import workflow
from ray.tests.conftest import *  # noqa


def test_workflow_error_message(shutdown_only):
    storage_url = r"c:\ray"
    expected_error_msg = f"Cannot parse URI: '{storage_url}'"
    if os.name == "nt":

        expected_error_msg += (
            " Try using file://{} or file:///{} for Windows file paths.".format(
                storage_url, storage_url
            )
        )
    ray.shutdown()
    with pytest.raises(ValueError) as e:
        ray.init(storage=storage_url)
    assert str(e.value) == expected_error_msg


def test_options_update(shutdown_only):
    from ray.workflow.common import WORKFLOW_OPTIONS

    # Options are given in decorator first, then in the first .options()
    # and finally in the second .options()
    @workflow.options(task_id="old_name", metadata={"k": "v"})
    @ray.remote(num_cpus=2, max_retries=1)
    def f():
        return

    # name is updated from the old name in the decorator to the new name in the first
    # .options(), then preserved in the second options.
    # metadata and ray_options are "updated"
    # max_retries only defined in the decorator and it got preserved all the way
    new_f = f.options(
        num_returns=2,
        **workflow.options(task_id="new_name", metadata={"extra_k2": "extra_v2"}),
    )
    options = new_f.bind().get_options()
    assert options == {
        "num_cpus": 2,
        "num_returns": 2,
        "max_retries": 1,
        "_metadata": {
            WORKFLOW_OPTIONS: {
                "task_id": "new_name",
                "metadata": {"extra_k2": "extra_v2"},
            }
        },
    }


def test_no_init_run(shutdown_only):
    # workflow should be able to run without explicit init
    @ray.remote
    def f():
        pass

    workflow.run(f.bind())


def test_no_init_api(shutdown_only):
    workflow.list_all()


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
