import pytest

from ray.tests.conftest import *  # noqa
from ray import workflow


@workflow.step
def signature_check(a, b, c=1):
    pass


def test_signature_check(workflow_start_regular):
    with pytest.raises(TypeError):
        signature_check(1, 2)

    with pytest.raises(TypeError):
        signature_check.step(1)

    with pytest.raises(TypeError):
        signature_check.step(1, c=2)

    with pytest.raises(TypeError):
        signature_check.step(1, 2, d=3)

    with pytest.raises(TypeError):
        signature_check.step(1, 2, 3, 4)

    signature_check.step(1, 2, 3)
    signature_check.step(1, 2, c=3)
    signature_check.step(1, b=2, c=3)
    signature_check.step(a=1, b=2, c=3)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
