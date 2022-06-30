import pytest

from ray.tests.conftest import *  # noqa
import ray
from ray import workflow


@ray.remote
def signature_check(a, b, c=1):
    pass


def test_signature_check(workflow_start_regular):
    with pytest.raises(TypeError):
        signature_check(1, 2)

    # TODO(suquark): Ray DAG does not check the inputs. Fix it in Ray DAG.
    with pytest.raises(TypeError):
        workflow.create(signature_check.bind(1)).run()

    with pytest.raises(TypeError):
        workflow.create(signature_check.bind(1, c=2)).run()

    with pytest.raises(TypeError):
        workflow.create(signature_check.bind(1, 2, d=3)).run()

    with pytest.raises(TypeError):
        workflow.create(signature_check.bind(1, 2, 3, 4)).run()

    workflow.create(signature_check.bind(1, 2, 3)).run()
    workflow.create(signature_check.bind(1, 2, c=3)).run()
    workflow.create(signature_check.bind(1, b=2, c=3)).run()
    workflow.create(signature_check.bind(a=1, b=2, c=3)).run()


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
