import pytest

from ray.serve._private.common import (
    REPLICA_ID_FULL_ID_STR_PREFIX,
    DeploymentID,
    DeploymentStatus,
    DeploymentStatusInfo,
    DeploymentStatusTrigger,
    ReplicaID,
    RunningReplicaInfo,
)
from ray.serve._private.utils import get_random_string
from ray.serve.generated.serve_pb2 import (
    DeploymentStatusInfo as DeploymentStatusInfoProto,
)


def test_replica_id_formatting():
    deployment = "DeploymentA"
    unique_id = get_random_string()

    app = "my_app"
    replica_id = ReplicaID(
        unique_id, deployment_id=DeploymentID(name=deployment, app_name=app)
    )
    assert (
        replica_id.to_full_id_str()
        == f"{REPLICA_ID_FULL_ID_STR_PREFIX}{app}#{deployment}#{unique_id}"
    )
    assert (
        str(replica_id)
        == f"Replica(id='{unique_id}', deployment='{deployment}', app='{app}')"
    )


def test_replica_id_from_str():
    unique_id = get_random_string()

    # Test without app name.
    full_id_str = f"{REPLICA_ID_FULL_ID_STR_PREFIX}DeploymentA#{unique_id}"
    replica_id = ReplicaID.from_full_id_str(full_id_str)
    assert replica_id.unique_id == unique_id
    assert replica_id.deployment_id == DeploymentID(name="DeploymentA", app_name="")

    # Test with app name.
    full_id_str = f"{REPLICA_ID_FULL_ID_STR_PREFIX}App1#DeploymentA#{unique_id}"
    replica_id = ReplicaID.from_full_id_str(full_id_str)
    assert replica_id.unique_id == unique_id
    assert replica_id.deployment_id == DeploymentID(name="DeploymentA", app_name="App1")


def test_invalid_id_from_str():
    unique_id = get_random_string()

    # Missing prefix.
    full_id_str = f"DeploymentA#{unique_id}"
    with pytest.raises(AssertionError):
        ReplicaID.from_full_id_str(full_id_str)

    # Too many delimiters.
    full_id_str = f"{REPLICA_ID_FULL_ID_STR_PREFIX}DeploymentA###{unique_id}"
    with pytest.raises(ValueError):
        ReplicaID.from_full_id_str(full_id_str)


def test_is_replica_id():
    unique_id = get_random_string()

    assert not ReplicaID.is_full_id_str(f"DeploymentA##{unique_id}")
    assert not ReplicaID.is_full_id_str(f"DeploymentA#{unique_id}")
    assert ReplicaID.is_full_id_str(
        f"{REPLICA_ID_FULL_ID_STR_PREFIX}DeploymentA#{unique_id}"
    )
    assert ReplicaID.is_full_id_str(
        f"{REPLICA_ID_FULL_ID_STR_PREFIX}#App1#DeploymentA#{unique_id}"
    )


class TestDeploymentStatusInfo:
    def test_name_required(self):
        with pytest.raises(TypeError):
            DeploymentStatusInfo(
                status=DeploymentStatus.HEALTHY,
                status_trigger=DeploymentStatusTrigger.CONFIG_UPDATE_STARTED,
            )

    def test_deployment_status_required(self):
        with pytest.raises(TypeError):
            DeploymentStatusInfo(
                name="test_name",
                status_trigger=DeploymentStatusTrigger.CONFIG_UPDATE_STARTED,
            )

    @pytest.mark.parametrize(
        "status,status_trigger",
        list(zip(list(DeploymentStatus), list(DeploymentStatusTrigger))),
    )
    def test_proto(self, status, status_trigger):
        deployment_status_info = DeploymentStatusInfo(
            name="test_name",
            status=status,
            status_trigger=status_trigger,
            message="context about status",
        )
        serialized_proto = deployment_status_info.to_proto().SerializeToString()
        deserialized_proto = DeploymentStatusInfoProto.FromString(serialized_proto)
        reconstructed_info = DeploymentStatusInfo.from_proto(deserialized_proto)

        assert deployment_status_info == reconstructed_info


def test_running_replica_info():
    """Test hash value of RunningReplicaInfo"""

    replica_id = ReplicaID("asdf123", deployment_id=DeploymentID(name="my_deployment"))
    actor_name = replica_id.to_full_id_str()

    # Test that replicas with same attributes have same hash
    replica1 = RunningReplicaInfo(
        replica_id=replica_id,
        node_id="node_id",
        node_ip="node_ip",
        availability_zone="some-az",
        actor_name=actor_name,
        max_ongoing_requests=1,
        is_cross_language=False,
    )
    replica2 = RunningReplicaInfo(
        replica_id=replica_id,
        node_id="node_id",
        node_ip="node_ip",
        availability_zone="some-az",
        actor_name=actor_name,
        max_ongoing_requests=1,
        is_cross_language=False,
    )
    # Test that cross-language setting affects hash
    replica3 = RunningReplicaInfo(
        replica_id=replica_id,
        node_id="node_id",
        node_ip="node_ip",
        availability_zone="some-az",
        actor_name=actor_name,
        max_ongoing_requests=1,
        is_cross_language=True,
    )
    assert replica1._hash == replica2._hash
    assert replica3._hash != replica1._hash


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-s", __file__]))
