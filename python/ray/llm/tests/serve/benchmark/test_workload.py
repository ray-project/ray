"""Tests for WorkloadSpec solver."""
import sys

import pytest

from ray.llm._internal.serve.benchmark.models import WorkloadSpec


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
                hit_rate=0.0,
                num_turns=1,
                osl=100,
                num_sessions=10,
            ).resolve()

    def test_both_traffic_modes_raises(self):
        """Setting both concurrency and request_rate should raise."""
        with pytest.raises(ValueError, match="Cannot specify both"):
            WorkloadSpec(
                isl=1000,
                hit_rate=0.0,
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

    def test_infeasible_user_tokens_suggests_lower_hit_rate_and_boundary_resolves(self):
        """user_tokens < 0.5: error names user_tokens and suggests a feasible hit_rate cap."""
        base = dict(
            isl=1000,
            hit_rate=1.0,
            num_turns=5,
            osl=100,
            shared_system_prompt_ratio=1.0,
            concurrency=1,
            num_sessions=1,
        )
        with pytest.raises(ValueError) as exc_info:
            WorkloadSpec(**base).resolve()
        msg = str(exc_info.value)
        assert "user_tokens" in msg
        assert "hit_rate <=" in msg
        # Suggested boundary (from _feasibility_suggestions) must resolve when applied.
        WorkloadSpec(**{**base, "hit_rate": 0.74}).resolve()

    def test_infeasible_sys_tokens_suggests_num_turns_and_boundary_resolves(self):
        """sys_tokens < -0.5: error names sys_tokens; reducing num_turns can fix."""
        base = dict(
            isl=500,
            hit_rate=0.99,
            num_turns=8,
            osl=200,
            shared_system_prompt_ratio=0.9,
            concurrency=1,
            num_sessions=1,
        )
        with pytest.raises(ValueError) as exc_info:
            WorkloadSpec(**base).resolve()
        msg = str(exc_info.value)
        assert "sys_tokens" in msg
        assert "num_turns <=" in msg
        WorkloadSpec(**{**base, "num_turns": 5}).resolve()

    def test_infeasible_no_single_parameter_fix(self):
        """Some parameter sets are infeasible with no one-dimensional remedy."""
        kw = dict(
            isl=442,
            hit_rate=0.39,
            num_turns=15,
            osl=216,
            shared_system_prompt_ratio=0.04,
            concurrency=1,
            num_sessions=1,
        )
        with pytest.raises(ValueError) as exc_info:
            WorkloadSpec(**kw).resolve()
        assert "no single-parameter fix found" in str(exc_info.value)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
