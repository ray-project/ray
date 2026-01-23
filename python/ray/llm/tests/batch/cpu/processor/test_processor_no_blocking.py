"""
Test that Ray Data LLM does not override wait_for_min_actors_s.

With default settings (wait_for_min_actors_s <= 0), processing starts
as soon as any actor is ready, regardless of concurrency config.
"""
import sys

import pytest

from ray.data import DataContext
from ray.llm._internal.batch.processor import ProcessorBuilder
from ray.llm._internal.batch.processor.vllm_engine_proc import vLLMEngineProcessorConfig


@pytest.fixture(autouse=True)
def reset_data_context():
    """Reset DataContext before and after each test."""
    ctx = DataContext.get_current()
    original_value = ctx.wait_for_min_actors_s
    ctx.wait_for_min_actors_s = -1
    yield
    ctx.wait_for_min_actors_s = original_value


class TestWaitForMinActorsNotOverridden:
    """Test that Processor does not override wait_for_min_actors_s."""

    def test_processor_does_not_override_default(self):
        """Processor should not change wait_for_min_actors_s from default."""
        ctx = DataContext.get_current()
        ctx.wait_for_min_actors_s = -1

        config = vLLMEngineProcessorConfig(
            model_source="facebook/opt-125m",
            concurrency=4,
        )
        ProcessorBuilder.build(config)

        assert ctx.wait_for_min_actors_s == -1

    @pytest.mark.parametrize("user_value", [60, 600, 1800])
    def test_processor_preserves_user_setting(self, user_value):
        """Processor should preserve user-set wait_for_min_actors_s."""
        ctx = DataContext.get_current()
        ctx.wait_for_min_actors_s = user_value

        config = vLLMEngineProcessorConfig(
            model_source="facebook/opt-125m",
            concurrency=4,
        )
        ProcessorBuilder.build(config)

        assert ctx.wait_for_min_actors_s == user_value


class TestConcurrencyConfigPassthrough:
    """
    Test that concurrency config correctly sets ActorPoolStrategy.

    This determines blocking behavior when wait_for_min_actors_s > 0:
    - concurrency=N → min_size=N → blocks for N actors
    - concurrency=(1, N) → min_size=1 → blocks for 1 actor
    """

    @pytest.mark.parametrize(
        "concurrency,expected_min_size,expected_max_size",
        [
            (4, 4, 4),  # int: fixed pool
            ((1, 4), 1, 4),  # tuple: autoscaling pool
            ((2, 8), 2, 8),  # tuple: custom min
        ],
        ids=["int_concurrency", "tuple_1_to_n", "tuple_custom_min"],
    )
    def test_concurrency_to_actor_pool_strategy(
        self, concurrency, expected_min_size, expected_max_size
    ):
        """Verify concurrency config maps to correct ActorPoolStrategy."""
        config = vLLMEngineProcessorConfig(
            model_source="facebook/opt-125m",
            concurrency=concurrency,
        )
        processor = ProcessorBuilder.build(config)

        # Get the vLLM stage and check its compute strategy
        stage = processor.get_stage_by_name("vLLMEngineStage")
        compute = stage.map_batches_kwargs.get("compute")

        assert (
            compute.min_size == expected_min_size
        ), f"Expected min_size={expected_min_size}, got {compute.min_size}"
        assert (
            compute.max_size == expected_max_size
        ), f"Expected max_size={expected_max_size}, got {compute.max_size}"


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
