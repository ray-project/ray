import time
import pytest

from ray.serve.utils import get_random_letters
from ray.serve.common import (
    ReplicaName,
    StatusOverview,
    DeploymentStatus,
    DeploymentStatusInfo,
    ApplicationStatus,
    ApplicationStatusInfo,
)
from ray.serve.generated.serve_pb2 import (
    StatusOverview as StatusOverviewProto,
    DeploymentStatusInfo as DeploymentStatusInfoProto,
    ApplicationStatusInfo as ApplicationStatusInfoProto,
)


def test_replica_tag_formatting():
    deployment_tag = "DeploymentA"
    replica_suffix = get_random_letters()

    replica_name = ReplicaName(deployment_tag, replica_suffix)
    assert replica_name.replica_tag == f"{deployment_tag}#{replica_suffix}"
    assert str(replica_name) == f"{deployment_tag}#{replica_suffix}"


def test_replica_name_from_str():
    replica_suffix = get_random_letters()
    actor_name = f"{ReplicaName.prefix}DeploymentA#{replica_suffix}"

    replica_name = ReplicaName.from_str(actor_name)
    assert (
        str(replica_name)
        == replica_name.replica_tag
        == actor_name.replace(ReplicaName.prefix, "")
    )


def test_invalid_name_from_str():
    replica_suffix = get_random_letters()

    replica_tag = f"DeploymentA##{replica_suffix}"
    with pytest.raises(AssertionError):
        ReplicaName.from_str(replica_tag)

    # No prefix
    replica_tag = f"DeploymentA#{replica_suffix}"
    with pytest.raises(AssertionError):
        ReplicaName.from_str(replica_tag)


def test_is_replica_name():
    replica_suffix = get_random_letters()

    assert not ReplicaName.is_replica_name(f"DeploymentA##{replica_suffix}")
    assert not ReplicaName.is_replica_name(f"DeploymentA#{replica_suffix}")
    assert ReplicaName.is_replica_name(
        f"{ReplicaName.prefix}DeploymentA#{replica_suffix}"
    )


class TestDeploymentStatusInfo:
    def test_name_required(self):
        with pytest.raises(TypeError):
            DeploymentStatusInfo(status=DeploymentStatus.HEALTHY)

    def test_deployment_status_required(self):
        with pytest.raises(TypeError):
            DeploymentStatusInfo(name="test_name")

    @pytest.mark.parametrize("status", list(DeploymentStatus))
    def test_proto(self, status):
        deployment_status_info = DeploymentStatusInfo(
            name="test_name", status=status, message="context about status"
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
                DeploymentStatusInfo(name="1", status=DeploymentStatus.HEALTHY),
                DeploymentStatusInfo(name="2", status=DeploymentStatus.UNHEALTHY),
            ],
        )

        status_info_many_deployments = StatusOverview(
            app_status=self.get_valid_serve_application_status_info(),
            deployment_statuses=[
                DeploymentStatusInfo(name="1", status=DeploymentStatus.HEALTHY),
                DeploymentStatusInfo(name="2", status=DeploymentStatus.UNHEALTHY),
                DeploymentStatusInfo(name="3", status=DeploymentStatus.UNHEALTHY),
                DeploymentStatusInfo(name="4", status=DeploymentStatus.UPDATING),
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
                ),
                DeploymentStatusInfo(
                    name="name2", status=DeploymentStatus.HEALTHY, message=""
                ),
                DeploymentStatusInfo(
                    name="name3",
                    status=DeploymentStatus.UNHEALTHY,
                    message="this deployment is unhealthy",
                ),
            ],
        )
        serialized_proto = status_info.to_proto().SerializeToString()
        deserialized_proto = StatusOverviewProto.FromString(serialized_proto)
        reconstructed_info = StatusOverview.from_proto(deserialized_proto)

        assert status_info == reconstructed_info


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-s", __file__]))
