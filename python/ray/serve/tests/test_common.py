import pytest

from ray.serve.utils import get_random_letters
from ray.serve.common import ReplicaName


def test_replica_tag_formatting():
    deployment_tag = "DeploymentA"
    replica_suffix = get_random_letters()

    replica_name = ReplicaName(deployment_tag, replica_suffix)
    assert replica_name.replica_tag == f"{deployment_tag}#{replica_suffix}"
    assert str(replica_name) == f"{deployment_tag}#{replica_suffix}"


def test_replica_name_from_str():
    replica_suffix = get_random_letters()
    replica_tag = f"DeploymentA#{replica_suffix}"

    replica_name = ReplicaName.from_str(replica_tag)
    assert replica_name.replica_tag == replica_tag
    assert str(replica_tag) == replica_tag


def test_invalid_name_from_str():
    replica_suffix = get_random_letters()
    replica_tag = f"DeploymentA##{replica_suffix}"

    with pytest.raises(AssertionError):
        ReplicaName.from_str(replica_tag)


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main(["-v", "-s", __file__]))
