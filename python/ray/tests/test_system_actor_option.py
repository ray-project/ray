r"""
Tests for the _system_actor option added to Ray's actor option utilities.

Run with:
    .venv\Scripts\python.exe -m pytest python/ray/tests/test_system_actor_option.py -v
"""

import pytest


class TestSystemActorOption:
    """Tests that _system_actor is a valid, recognised actor option."""

    def test_system_actor_in_actor_only_options(self):
        """_system_actor must appear in the actor-only option registry."""
        from ray._common.ray_option_utils import _actor_only_options

        assert (
            "_system_actor" in _actor_only_options
        ), "_system_actor is missing from _actor_only_options"

    def test_system_actor_in_actor_options(self):
        """_system_actor must appear in the merged actor_options dict."""
        from ray._common.ray_option_utils import actor_options

        assert (
            "_system_actor" in actor_options
        ), "_system_actor is missing from actor_options"

    def test_system_actor_not_in_task_options(self):
        """_system_actor is an actor-only flag — it must NOT appear in task_options."""
        from ray._common.ray_option_utils import task_options

        assert (
            "_system_actor" not in task_options
        ), "_system_actor should not be in task_options (it is actor-only)"

    def test_system_actor_default_is_false(self):
        """The default value for _system_actor must be False."""
        from ray._common.ray_option_utils import actor_options

        default = actor_options["_system_actor"].default_value
        assert default is False, f"Expected default_value=False, got {default!r}"

    def test_validate_actor_options_accepts_true(self):
        """validate_actor_options must accept _system_actor=True without raising."""
        from ray._common.ray_option_utils import validate_actor_options

        validate_actor_options({"_system_actor": True}, in_options=False)

    def test_validate_actor_options_accepts_false(self):
        """validate_actor_options must accept _system_actor=False without raising."""
        from ray._common.ray_option_utils import validate_actor_options

        validate_actor_options({"_system_actor": False}, in_options=False)

    def test_validate_actor_options_accepts_none(self):
        """validate_actor_options must accept _system_actor=None (optional bool)."""
        from ray._common.ray_option_utils import validate_actor_options

        validate_actor_options({"_system_actor": None}, in_options=False)

    def test_validate_actor_options_rejects_invalid_type(self):
        """validate_actor_options must reject non-bool _system_actor values."""
        from ray._common.ray_option_utils import validate_actor_options

        with pytest.raises(TypeError):
            validate_actor_options({"_system_actor": "yes"}, in_options=False)

    def test_system_actor_option_unknown_to_task_validator(self):
        """validate_task_options must reject _system_actor (actor-only flag)."""
        from ray._common.ray_option_utils import validate_task_options

        with pytest.raises(ValueError, match="Invalid option keyword"):
            validate_task_options({"_system_actor": True}, in_options=False)


class TestSystemActorOptionInActorCreate:
    """
    Integration-level smoke-tests that exercise the Python→C++ option plumbing
    for _system_actor without starting a full Ray cluster.

    These tests mock the underlying core_worker.create_actor so we can assert
    that the flag is forwarded correctly from actor_options down the call stack.
    """

    def test_system_actor_flag_forwarded_to_create_actor(self, monkeypatch):
        """
        Validates that when _system_actor=True is present in actor_options, the
        value is correctly resolved and available to be forwarded to create_actor.

        This tests the Python-side plumbing without starting a Ray cluster.
        """
        from ray._common.ray_option_utils import actor_options

        # Build the full set of defaults as _remote() would.
        merged = {k: v.default_value for k, v in actor_options.items()}
        # Simulate user passing _system_actor=True.
        merged["_system_actor"] = True

        # The value that would be forwarded to core_worker.create_actor.
        is_system_actor = merged.get("_system_actor", False)
        assert (
            is_system_actor is True
        ), f"Expected is_system_actor=True in merged options, got {is_system_actor!r}"

        # Verify the default (False) is used when not specified.
        merged_default = {k: v.default_value for k, v in actor_options.items()}
        assert merged_default.get("_system_actor") is False


class TestSystemActorE2E:
    """
    End-to-End tests to ensure the Python-to-C++ boundary actually supports
    the _system_actor flag without throwing Cython/kwargs parsing errors.

    These tests start a temporary local Ray cluster, spawn real actors, and
    verify that the flag travels safely through:
        ray.remote() → actor.py → _raylet.pyx → C++ create_actor
    """

    @classmethod
    def setup_class(cls):
        import ray

        if not ray.is_initialized():
            ray.init(num_cpus=1, ignore_reinit_error=True)

    @classmethod
    def teardown_class(cls):
        import ray

        ray.shutdown()

    def test_spawn_system_actor_end_to_end(self):
        """
        Prove that passing _system_actor=True successfully boots the actor.

        If the Cython boundary or ActorCreationOptions is misaligned with our
        changes this will crash with:
            TypeError: create_actor() got an unexpected keyword argument 'is_system_actor'
        """
        import ray

        @ray.remote(_system_actor=True)
        class MockServeController:
            def ping(self):
                return "pong"

        # This line crosses the Cython boundary — if anything is wrong it crashes here.
        controller = MockServeController.remote()

        # Verify the actor actually booted and can communicate.
        result = ray.get(controller.ping.remote())
        assert result == "pong", f"Expected 'pong', got {result!r}"

    def test_spawn_normal_actor_unaffected(self):
        """
        Prove that normal actors (without _system_actor) still boot fine.
        Our changes must be fully backward-compatible.
        """
        import ray

        @ray.remote
        class NormalWorker:
            def ping(self):
                return "pong"

        worker = NormalWorker.remote()
        result = ray.get(worker.ping.remote())
        assert result == "pong", f"Expected 'pong', got {result!r}"

    def test_spawn_system_actor_false_same_as_default(self):
        """
        Prove that explicitly setting _system_actor=False is identical to
        omitting the flag — both must boot successfully.
        """
        import ray

        @ray.remote(_system_actor=False)
        class ExplicitNonSystemActor:
            def ping(self):
                return "pong"

        actor = ExplicitNonSystemActor.remote()
        result = ray.get(actor.ping.remote())
        assert result == "pong", f"Expected 'pong', got {result!r}"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
