"""Tests for WorkloadSpec solver."""
import sys

import pytest

from ray.llm._internal.serve.benchmark.multiturn_bench import WorkloadSpec


class TestWorkloadSpec:
    """Test WorkloadSpec resolution and invariants."""

    def test_single_turn(self):
        """Single turn: ISL round-trip."""
        spec = WorkloadSpec(
            isl=1000,
            hit_rate=0.5,
            num_turns=1,
            osl=100,
            shared_system_prompt_ratio=1.0,
            concurrency=1,
            num_sessions=1,
        ).resolve()
        assert abs(spec.effective_isl - 1000) < 2
        assert abs(spec.effective_h - 0.5) < 0.02

    def test_multi_turn_full_sharing(self):
        """Multi-turn with full cross-sharing."""
        spec = WorkloadSpec(
            isl=5600,
            hit_rate=0.8,
            num_turns=5,
            osl=140,
            shared_system_prompt_ratio=1.0,
            concurrency=8,
            num_sessions=100,
        ).resolve()
        assert abs(spec.effective_isl - 5600) < 50
        assert abs(spec.effective_h - 0.8) < 0.02

    def test_multi_turn_partial_sharing(self):
        """Multi-turn with partial cross-sharing."""
        spec = WorkloadSpec(
            isl=3000,
            hit_rate=0.6,
            num_turns=3,
            osl=100,
            shared_system_prompt_ratio=0.5,
            concurrency=4,
            num_sessions=50,
        ).resolve()
        assert abs(spec.effective_isl - 3000) < 50
        assert abs(spec.effective_h - 0.6) < 0.05

    def test_rate_mode_with_duration(self):
        """Rate mode with duration (no num_sessions required)."""
        spec = WorkloadSpec(
            isl=2000,
            hit_rate=0.7,
            num_turns=3,
            osl=100,
            shared_system_prompt_ratio=0.5,
            request_rate=10.0,
            duration_s=60.0,
        ).resolve()
        assert spec.request_rate == 10.0

    def test_missing_isl_raises(self):
        """Missing ISL should raise."""
        with pytest.raises(ValueError, match="--isl.*--hit-rate"):
            WorkloadSpec(
                hit_rate=0.5,
                num_turns=1,
                osl=100,
                concurrency=1,
                num_sessions=1,
            ).resolve()

    def test_missing_traffic_raises(self):
        """Missing both concurrency and request_rate should raise."""
        with pytest.raises(ValueError, match="--concurrency.*--request-rate"):
            WorkloadSpec(
                isl=1000,
                hit_rate=0.5,
                num_turns=1,
                osl=100,
                num_sessions=10,
            ).resolve()

    def test_both_traffic_modes_raises(self):
        """Setting both concurrency and request_rate should raise."""
        with pytest.raises(ValueError, match="Cannot specify both"):
            WorkloadSpec(
                isl=1000,
                hit_rate=0.5,
                num_turns=1,
                osl=100,
                concurrency=4,
                request_rate=10.0,
                num_sessions=10,
            ).resolve()

    def test_per_turn_tokens_monotonic(self):
        """Input tokens should increase monotonically across turns."""
        spec = WorkloadSpec(
            isl=5600,
            hit_rate=0.8,
            num_turns=5,
            osl=140,
            concurrency=8,
            num_sessions=100,
        ).resolve()
        for k in range(2, spec.num_turns + 1):
            assert spec.turn_input_tokens(k) > spec.turn_input_tokens(k - 1)

    def test_summary_keys(self):
        """Summary dict should have expected keys."""
        spec = WorkloadSpec(
            isl=1000,
            hit_rate=0.5,
            num_turns=2,
            osl=100,
            concurrency=4,
            num_sessions=10,
        ).resolve()
        s = spec.summary()
        expected_keys = {
            "num_sessions",
            "duration_s",
            "num_turns",
            "osl",
            "think_time",
            "concurrency",
            "request_rate",
            "shared_system_prompt_ratio",
            "user_tokens_per_turn",
            "system_prompt_tokens",
            "shared_system_prompt",
            "unique_system_prompt",
            "effective_isl",
            "effective_hit_rate",
            "per_turn",
        }
        assert expected_keys.issubset(s.keys())
        assert len(s["per_turn"]) == 2


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
