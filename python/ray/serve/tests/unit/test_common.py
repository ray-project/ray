import time

import pytest

from ray.serve._private.common import (
    REPLICA_ID_FULL_ID_STR_PREFIX,
    ApplicationStatus,
    ApplicationStatusInfo,
    DeploymentID,
    DeploymentStatus,
    DeploymentStatusInfo,
    DeploymentStatusTrigger,
    ReplicaID,
    RunningReplicaInfo,
    StatusOverview,
)
from ray.serve._private.utils import get_random_string
from ray.serve.generated.serve_pb2 import (
    ApplicationStatusInfo as ApplicationStatusInfoProto,
)
from ray.serve.generated.serve_pb2 import (
    DeploymentStatusInfo as DeploymentStatusInfoProto,
)
from ray.serve.generated.serve_pb2 import StatusOverview as StatusOverviewProto


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


class TestApplicationStatusInfo:
    def test_application_status_required(self):
        with pytest.raises(TypeError):
            ApplicationStatusInfo(
                message="context about status", deployment_timestamp=time.time()
            )

    @pytest.mark.parametrize("status", list(ApplicationStatus))
    def test_proto(self, status):
        serve_application_status_info = ApplicationStatusInfo(
            status=status,
            message="context about status",
            deployment_timestamp=time.time(),
        )
        serialized_proto = serve_application_status_info.to_proto().SerializeToString()
        deserialized_proto = ApplicationStatusInfoProto.FromString(serialized_proto)
        reconstructed_info = ApplicationStatusInfo.from_proto(deserialized_proto)

        assert serve_application_status_info == reconstructed_info


class TestStatusOverview:
    def get_valid_serve_application_status_info(self):
        return ApplicationStatusInfo(
            status=ApplicationStatus.RUNNING,
            message="",
            deployment_timestamp=time.time(),
        )

    def test_app_status_required(self):
        with pytest.raises(TypeError):
            StatusOverview(deployment_statuses=[])

    def test_empty_list_valid(self):
        """Should be able to create StatusOverview with no deployment statuses."""

        # Check default is empty list
        status_info = StatusOverview(
            app_status=self.get_valid_serve_application_status_info()
        )
        status_info.deployment_statuses == []

        # Ensure empty list can be passed in explicitly
        status_info = StatusOverview(
            app_status=self.get_valid_serve_application_status_info(),
            deployment_statuses=[],
        )
        status_info.deployment_statuses == []

    def test_equality_mismatched_deployment_statuses(self):
        """Check that StatusOverviews with different numbers of statuses are unequal."""

        status_info_few_deployments = StatusOverview(
            app_status=self.get_valid_serve_application_status_info(),
            deployment_statuses=[
                DeploymentStatusInfo(
                    name="1",
                    status=DeploymentStatus.HEALTHY,
                    status_trigger=DeploymentStatusTrigger.CONFIG_UPDATE_STARTED,
                ),
                DeploymentStatusInfo(
                    name="2",
                    status=DeploymentStatus.UNHEALTHY,
                    status_trigger=DeploymentStatusTrigger.CONFIG_UPDATE_STARTED,
                ),
            ],
        )

        status_info_many_deployments = StatusOverview(
            app_status=self.get_valid_serve_application_status_info(),
            deployment_statuses=[
                DeploymentStatusInfo(
                    name="1",
                    status=DeploymentStatus.HEALTHY,
                    status_trigger=DeploymentStatusTrigger.CONFIG_UPDATE_STARTED,
                ),
                DeploymentStatusInfo(
                    name="2",
                    status=DeploymentStatus.UNHEALTHY,
                    status_trigger=DeploymentStatusTrigger.CONFIG_UPDATE_STARTED,
                ),
                DeploymentStatusInfo(
                    name="3",
                    status=DeploymentStatus.UNHEALTHY,
                    status_trigger=DeploymentStatusTrigger.CONFIG_UPDATE_STARTED,
                ),
                DeploymentStatusInfo(
                    name="4",
                    status=DeploymentStatus.UPDATING,
                    status_trigger=DeploymentStatusTrigger.CONFIG_UPDATE_STARTED,
                ),
            ],
        )

        assert status_info_few_deployments != status_info_many_deployments

    @pytest.mark.parametrize("application_status", list(ApplicationStatus))
    def test_proto(self, application_status):
        status_info = StatusOverview(
            app_status=ApplicationStatusInfo(
                status=application_status,
                message="context about this status",
                deployment_timestamp=time.time(),
            ),
            deployment_statuses=[
                DeploymentStatusInfo(
                    name="name1",
                    status=DeploymentStatus.UPDATING,
                    message="deployment updating",
                    status_trigger=DeploymentStatusTrigger.CONFIG_UPDATE_STARTED,
                ),
                DeploymentStatusInfo(
                    name="name2",
                    status=DeploymentStatus.HEALTHY,
                    message="",
                    status_trigger=DeploymentStatusTrigger.CONFIG_UPDATE_STARTED,
                ),
                DeploymentStatusInfo(
                    name="name3",
                    status=DeploymentStatus.UNHEALTHY,
                    message="this deployment is unhealthy",
                    status_trigger=DeploymentStatusTrigger.CONFIG_UPDATE_STARTED,
                ),
            ],
        )
        serialized_proto = status_info.to_proto().SerializeToString()
        deserialized_proto = StatusOverviewProto.FromString(serialized_proto)
        reconstructed_info = StatusOverview.from_proto(deserialized_proto)

        assert status_info == reconstructed_info


def test_running_replica_info():
    """Test hash value of RunningReplicaInfo"""

    class FakeActorHandler:
        def __init__(self, actor_id):
            self._actor_id = actor_id

    fake_h1 = FakeActorHandler("1")
    fake_h2 = FakeActorHandler("1")
    replica_id = ReplicaID("asdf123", deployment_id=DeploymentID(name="my_deployment"))
    assert fake_h1 != fake_h2
    replica1 = RunningReplicaInfo(
        replica_id, "node_id", "node_ip", "some-az", fake_h1, 1, False
    )
    replica2 = RunningReplicaInfo(
        replica_id, "node_id", "node_ip", "some-az", fake_h2, 1, False
    )
    replica3 = RunningReplicaInfo(
        replica_id, "node_id", "node_ip", "some-az", fake_h2, 1, True
    )
    assert replica1._hash == replica2._hash
    assert replica3._hash != replica1._hash


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-s", __file__]))
