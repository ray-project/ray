import sys
import pytest

from ray.exceptions import RayActorError
from ray.core.generated.common_pb2 import ActorDiedErrorContext
from ray.serve._private.utils import is_actor_dead


class TestIsActorDead:
    def test_actor_died_error_message(self):
        """Check that is_actor_dead returns True when an actor has died."""

        context = ActorDiedErrorContext(
            error_message="Fake error message",
            actor_id=("a" * 16).encode("utf-8"),
            class_name="Fake class name"
        )

        # If a RayActorError has a cause, then the actor has died.
        ray_actor_error_with_cause = RayActorError(cause=context)
        assert is_actor_dead(ray_actor_error_with_cause) is True
    
    def test_actor_died_empty_error_message(self):
        """Check that is_actor_dead returns True when the context is empty."""

        # A RayActorError's context must contain at least an actor ID.
        with pytest.raises(ValueError, match="ID string needs to have length"):
            context = ActorDiedErrorContext()
            RayActorError(cause=context)

        context = ActorDiedErrorContext(actor_id=("a" * 16).encode("utf-8"))

        # If a RayActorError has a cause, then the actor has died.
        ray_actor_error_with_cause = RayActorError(cause=context)
        assert is_actor_dead(ray_actor_error_with_cause) is True

    def test_slow_network(self):
        """Check that is_actor_dead returns False when network is slow."""

        # A slow network returns a RayActorError with no cause.
        empty_ray_actor_error = RayActorError()
        assert is_actor_dead(empty_ray_actor_error) is False
    
    def test_input_validation(self):
        """Check that is_actor_dead validates the error is a RayActorError."""
        
        runtime_error = RuntimeError("This is not a RayActorError!")
        
        # Only a RayActorError indicates that the actor has died.
        assert is_actor_dead(runtime_error) is False
    
    def test_unexpected_error(self):
        """Check that an unexpected error in is_actor_dead doesn't cause a crash."""

        class BrokenRayActorError(RayActorError):
            """This custom RayActorError removes its own error messages."""
            
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                del self.base_error_msg
        
        context = ActorDiedErrorContext(
            error_message="Fake error message",
            actor_id=("a" * 16).encode("utf-8"),
            class_name="Fake class name"
        )

        broken_ray_actor_error = BrokenRayActorError(cause=context)
        
        # is_actor_dead should return False since we don't know whether the
        # actor has died for sure.
        assert is_actor_dead(broken_ray_actor_error) is False


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))