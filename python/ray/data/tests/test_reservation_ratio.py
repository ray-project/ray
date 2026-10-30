from types import SimpleNamespace

import pytest

from ray.data._internal.cbo_stats.operator_statistics import (
    ConfidenceLevel,
    OperatorStatistics,
)
from ray.data._internal.cbo_stats.reservation_ratio import ReservationRatioDeriver
from ray.data._internal.execution import resolve_reservation_ratio
from ray.data._internal.logical.rules.derive_reservation_ratio_rule import (
    DeriveReservationRatioRule,
    _collect_demands,
)
from ray.data.context import DEFAULT_OP_RESOURCE_RESERVATION_RATIO, DataContext

HIGH = ConfidenceLevel.HIGH.value
DEFAULT = 0.5


class TestReservationRatioDeriver:
    def test_even_demand_keeps_default(self):
        decision = ReservationRatioDeriver.derive([100, 100, 100], 1.0, DEFAULT)
        assert decision.ratio == DEFAULT
        assert decision.evenness == pytest.approx(1.0)

    def test_skewed_demand_lowers_ratio(self):
        decision = ReservationRatioDeriver.derive([1000, 1, 1], 1.0, DEFAULT)
        assert ReservationRatioDeriver.MIN_RATIO <= decision.ratio < DEFAULT

    def test_monotone_in_skew(self):
        ratios = [
            ReservationRatioDeriver.derive([heavy, 10, 10], 1.0, DEFAULT).ratio
            for heavy in (10, 100, 1000, 10_000)
        ]
        assert ratios == sorted(ratios, reverse=True)

    def test_confidence_shrinks_towards_default(self):
        demands = [1000, 1, 1]
        confident = ReservationRatioDeriver.derive(demands, 1.0, DEFAULT).ratio
        unsure = ReservationRatioDeriver.derive(demands, 0.6, DEFAULT).ratio
        assert confident < unsure < DEFAULT

    def test_never_exceeds_default(self):
        for default in (0.1, 0.3, 0.5, 0.9):
            for demands in ([1, 1], [1, 100], [5, 0, 5]):
                ratio = ReservationRatioDeriver.derive(demands, 1.0, default).ratio
                assert min(ReservationRatioDeriver.MIN_RATIO, default) <= ratio
                assert ratio <= default

    @pytest.mark.parametrize(
        "demands,confidence",
        [
            ([100], 1.0),
            ([100, None], 1.0),
            ([100, -1], 1.0),
            ([0, 0], 1.0),
            ([1000, 1], 0.0),
        ],
    )
    def test_no_decision(self, demands, confidence):
        decision = ReservationRatioDeriver.derive(demands, confidence, DEFAULT)
        assert decision.ratio is None
        assert decision.reason

    def test_decayed_low_confidence_still_participates(self):
        # A UDF prior is capped at LOW and then decays by 0.9 per operator, so a
        # three-operator chain lands at 0.4 * 0.9**3. It must still be able to
        # move R; otherwise UDF pipelines silently fall back to the default.
        ratio = ReservationRatioDeriver.derive([24000, 8000], 0.2916, DEFAULT).ratio
        assert ratio is not None
        assert ReservationRatioDeriver.MIN_RATIO <= ratio < DEFAULT

    def test_low_confidence_decision_is_heavily_shrunk(self):
        ratio = ReservationRatioDeriver.derive(
            [1000, 1, 1], ConfidenceLevel.LOW.value, DEFAULT
        ).ratio
        assert ReservationRatioDeriver.MIN_RATIO <= ratio < DEFAULT
        assert DEFAULT - ratio <= ConfidenceLevel.LOW.value * (
            DEFAULT - ReservationRatioDeriver.MIN_RATIO
        )

    def test_evenness(self):
        assert ReservationRatioDeriver.evenness([1, 1, 1, 1]) == pytest.approx(1.0)
        assert ReservationRatioDeriver.evenness([1, 0, 0]) == pytest.approx(0.0)
        assert 0 < ReservationRatioDeriver.evenness([3, 1]) < 1


class _LogicalOp:
    def __init__(self, stats):
        self._stats = stats

    def infer_statistics(self):
        return self._stats


class _PhysicalOp:
    def __init__(
        self,
        *inputs,
        size_bytes=None,
        confidence=HIGH,
        bounded=True,
        throttling=True,
    ):
        self.input_dependencies = list(inputs)
        self._throttling = throttling
        stats = (
            OperatorStatistics(
                size_bytes=size_bytes,
                size_bytes_upper=size_bytes if bounded else None,
                confidence=confidence,
            )
            if size_bytes is not None
            else None
        )
        self._logical_operators = [_LogicalOp(stats)]

    def throttling_disabled(self):
        return not self._throttling


def _context(**kwargs):
    ctx = DataContext()
    ctx.enable_cost_based_optimization = True
    for key, value in kwargs.items():
        setattr(ctx, key, value)
    return ctx


def _skewed_dag():
    read = _PhysicalOp(size_bytes=1000)
    filtered = _PhysicalOp(read, size_bytes=10)
    return _PhysicalOp(filtered, size_bytes=10)


class TestCollectDemands:
    def test_linear(self):
        demands, confidence = _collect_demands(_skewed_dag())
        assert sorted(demands) == [10, 10, 1000]
        assert confidence == HIGH

    def test_ineligible_ops_are_charged_upstream(self):
        read = _PhysicalOp(size_bytes=100)
        shuffle = _PhysicalOp(read, size_bytes=50, throttling=False)
        sink = _PhysicalOp(shuffle, size_bytes=10)
        demands, _ = _collect_demands(sink)
        assert sorted(demands) == [10, 150]

    def test_unknown_size(self):
        read = _PhysicalOp(size_bytes=100)
        demands, confidence = _collect_demands(_PhysicalOp(read))
        assert None in demands
        assert confidence == 0.0

    def test_unbounded_estimate_is_capped_at_low(self):
        read = _PhysicalOp(size_bytes=100)
        unbounded = _PhysicalOp(read, size_bytes=10, confidence=HIGH, bounded=False)
        demands, confidence = _collect_demands(unbounded)
        assert sorted(demands) == [10, 100]
        assert confidence == ConfidenceLevel.LOW.value

    def test_diamond_is_visited_once(self):
        read = _PhysicalOp(size_bytes=100)
        left = _PhysicalOp(read, size_bytes=10)
        right = _PhysicalOp(read, size_bytes=10)
        demands, _ = _collect_demands(_PhysicalOp(left, right, size_bytes=20))
        assert sorted(demands) == [10, 10, 20, 100]

    def test_shared_ineligible_downstream_is_charged_once(self):
        read = _PhysicalOp(size_bytes=100)
        branch_a = _PhysicalOp(read, size_bytes=10, throttling=False)
        branch_b = _PhysicalOp(read, size_bytes=10, throttling=False)
        shared = _PhysicalOp(branch_a, branch_b, size_bytes=5, throttling=False)
        demands, _ = _collect_demands(shared)
        # ``shared`` is reachable through both ineligible branches but its size
        # must only be charged to ``read`` once (100 + 10 + 10 + 5).
        assert demands == [125]


class TestDeriveReservationRatioRule:
    def _apply(self, ctx, dag=None):
        plan = SimpleNamespace(context=ctx, dag=dag or _skewed_dag())
        assert DeriveReservationRatioRule().apply(plan) is plan
        return ctx._cbo_derived_reservation_ratio

    def test_derives_ratio(self):
        ratio = self._apply(_context())
        assert ratio is not None and ratio < DEFAULT_OP_RESOURCE_RESERVATION_RATIO

    def test_disabled_by_default(self):
        ctx = DataContext()
        assert not ctx.enable_cost_based_optimization
        assert self._apply(ctx) is None

    def test_user_ratio_wins(self):
        ctx = _context(op_resource_reservation_ratio=0.7)
        assert ctx._user_set_reservation_ratio
        assert self._apply(ctx) is None

    def test_resource_reservation_disabled(self):
        assert self._apply(_context(op_resource_reservation_enabled=False)) is None

    def test_resets_stale_value(self):
        ctx = _context()
        ctx._cbo_derived_reservation_ratio = 0.33
        unknown_confidence = _PhysicalOp(
            _PhysicalOp(size_bytes=1000, confidence=ConfidenceLevel.UNKNOWN.value),
            size_bytes=1,
        )
        assert self._apply(ctx, unknown_confidence) is None

    def test_errors_keep_default(self):
        class _Broken(_PhysicalOp):
            @property
            def _logical_operators(self):
                raise RuntimeError

            @_logical_operators.setter
            def _logical_operators(self, value):
                pass

        assert self._apply(_context(), _Broken(_PhysicalOp(size_bytes=1))) is None


class TestResolveReservationRatio:
    def test_precedence(self):
        ctx = _context()
        ctx._cbo_derived_reservation_ratio = 0.35
        assert resolve_reservation_ratio(ctx) == 0.35

        ctx.enable_cost_based_optimization = False
        assert resolve_reservation_ratio(ctx) == DEFAULT_OP_RESOURCE_RESERVATION_RATIO

        user = _context(op_resource_reservation_ratio=0.8)
        user._cbo_derived_reservation_ratio = 0.35
        assert resolve_reservation_ratio(user) == 0.8

    def test_invalid_derived_value(self):
        ctx = _context()
        ctx._cbo_derived_reservation_ratio = 1.5
        assert resolve_reservation_ratio(ctx) == DEFAULT_OP_RESOURCE_RESERVATION_RATIO


class TestUserSetTracking:
    def test_assignment_marks_user_set(self):
        ctx = DataContext()
        assert not ctx._user_set_reservation_ratio
        # Even assigning the default value is an explicit choice.
        ctx.op_resource_reservation_ratio = DEFAULT_OP_RESOURCE_RESERVATION_RATIO
        assert ctx._user_set_reservation_ratio

    def test_constructor_argument(self):
        assert DataContext(
            op_resource_reservation_ratio=0.2
        )._user_set_reservation_ratio

    def test_env_var(self, monkeypatch):
        monkeypatch.setenv("RAY_DATA_OP_RESERVATION_RATIO", "0.5")
        assert DataContext()._user_set_reservation_ratio

    def test_copy_preserves_flag(self):
        ctx = DataContext()
        assert not ctx.copy()._user_set_reservation_ratio
        ctx.op_resource_reservation_ratio = 0.4
        assert ctx.copy()._user_set_reservation_ratio


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
