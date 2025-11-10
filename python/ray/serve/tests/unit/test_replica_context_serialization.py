import sys

import pytest

from ray import cloudpickle as pickle
from ray.serve._private.common import DeploymentID, ReplicaID
from ray.serve._private.config import DeploymentConfig
from ray.serve.context import ReplicaContext


class TestReplicaContextSerialization:
    """Test that ReplicaContext can be serialized with pickle."""

    def test_pickle_without_callback(self):
        """Test pickling ReplicaContext without a callback."""
        replica_id = ReplicaID(
            "test-id", DeploymentID(name="test-deployment", app_name="test-app")
        )
        deployment_config = DeploymentConfig(num_replicas=2)

        ctx = ReplicaContext(
            replica_id=replica_id,
            servable_object=None,
            _deployment_config=deployment_config,
            rank=1,
            world_size=2,
            _handle_registration_callback=None,
        )

        # Serialize and deserialize
        serialized = pickle.dumps(ctx)
        deserialized = pickle.loads(serialized)

        # Verify all fields are preserved
        assert deserialized.replica_id == ctx.replica_id
        assert deserialized.servable_object == ctx.servable_object
        assert deserialized._deployment_config == ctx._deployment_config
        assert deserialized.rank == ctx.rank
        assert deserialized.world_size == ctx.world_size
        assert deserialized._handle_registration_callback is None

        # Verify properties work
        assert deserialized.app_name == "test-app"
        assert deserialized.deployment == "test-deployment"
        assert deserialized.replica_tag == "test-id"

    def test_pickle_with_callback(self):
        """Test that callback is excluded during serialization."""
        replica_id = ReplicaID(
            "test-id", DeploymentID(name="test-deployment", app_name="test-app")
        )
        deployment_config = DeploymentConfig(num_replicas=2)

        # Create a callback that captures local state (non-serializable)
        local_state = {"counter": 0}

        def test_callback(deployment_id):
            local_state["counter"] += 1

        ctx = ReplicaContext(
            replica_id=replica_id,
            servable_object=None,
            _deployment_config=deployment_config,
            rank=1,
            world_size=2,
            _handle_registration_callback=test_callback,
        )

        # Original context has callback
        assert ctx._handle_registration_callback is not None

        # Serialize and deserialize
        serialized = pickle.dumps(ctx)
        deserialized = pickle.loads(serialized)

        # Callback should be None after deserialization
        assert deserialized._handle_registration_callback is None

        # All other fields should be preserved
        assert deserialized.replica_id == ctx.replica_id
        assert deserialized.rank == ctx.rank
        assert deserialized.world_size == ctx.world_size

    def test_pickle_with_servable_object(self):
        """Test pickling with a servable object."""
        replica_id = ReplicaID(
            "test-id", DeploymentID(name="test-deployment", app_name="test-app")
        )
        deployment_config = DeploymentConfig(num_replicas=3)

        class UserClass:
            def __init__(self):
                self.value = 42

            def __call__(self):
                return self.value

        servable = UserClass()

        ctx = ReplicaContext(
            replica_id=replica_id,
            servable_object=servable,
            _deployment_config=deployment_config,
            rank=2,
            world_size=3,
        )

        # Serialize and deserialize
        serialized = pickle.dumps(ctx)
        deserialized = pickle.loads(serialized)

        # Verify servable object is preserved
        assert isinstance(deserialized.servable_object, UserClass)
        assert deserialized.servable_object.value == 42
        assert deserialized.servable_object() == 42


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
