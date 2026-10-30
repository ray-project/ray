import pytest

from ray.data._internal.cbo_stats.feedback_contract import (
    DEFAULT_MAX_RELATIVE_SPREAD,
    RECOMMENDED_SAMPLES,
    BoundaryObservation,
    ExecutionShape,
    FeedbackKey,
    evaluate_reuse,
)
from ray.data._internal.cbo_stats.operator_statistics import ConfidenceLevel

MEDIUM = ConfidenceLevel.MEDIUM.value
HIGH = ConfidenceLevel.HIGH.value


def _shape(**overrides):
    values = {
        "batch_size": 1024,
        "batch_format": "numpy",
        "coalesce_batches": True,
        "fused_operators": ("MapBatches", "MapBatches"),
        "execution_mode": "task",
        "state_scope": None,
    }
    values.update(overrides)
    return ExecutionShape(**values)


def _key(shape=None, **overrides):
    values = {
        "job_scope": "job-1",
        "data_version": "data-v3",
        "udf_version": "udf-v2",
        "operator": "MapBatches(predict)",
        "params_digest": "sha256:abc",
        "shape": shape or _shape(),
    }
    values.update(overrides)
    return FeedbackKey(**values)


def _observation(**overrides):
    values = {
        "key": _key(),
        "output_rows": 1000,
        "output_bytes": 8_000,
        "sample_count": RECOMMENDED_SAMPLES,
        "relative_spread": 0.02,
        "coverage": 1.0,
        "complete": True,
    }
    values.update(overrides)
    return BoundaryObservation(**values)


def test_reuses_a_complete_stable_boundary():
    decision = evaluate_reuse(
        _observation(),
        planned_key=_key(),
        repeatable=True,
    )

    assert decision.allowed is True
    assert decision.reasons == ()
    assert decision.confidence == pytest.approx(HIGH * (1 - 0.02 / 0.10), abs=1e-4)
    assert decision.safety_factor == pytest.approx(1.02)


def test_identity_match_with_different_execution_shape_is_rejected():
    # Same data/UDF versions, but the UDF is now called with a different batch
    # size and fusion; the observed output volume does not transfer.
    planned = _key(
        shape=_shape(batch_size=4096, fused_operators=("MapBatches(shuffle)",))
    )
    decision = evaluate_reuse(
        _observation(),
        planned_key=planned,
        repeatable=True,
    )

    assert decision.allowed is False
    assert any("execution shape changed" in reason for reason in decision.reasons)
    combined = " ".join(decision.reasons)
    assert "batch_size" in combined
    assert "fused_operators" in combined


def test_version_token_alone_is_not_enough():
    decision = evaluate_reuse(
        _observation(),
        planned_key=None,
        repeatable=True,
    )

    assert decision.allowed is False
    assert any("version token alone" in reason for reason in decision.reasons)


def test_unknown_shape_fields_are_rejected():
    decision = evaluate_reuse(
        _observation(key=_key(shape=_shape(batch_format=None))),
        planned_key=_key(shape=_shape(batch_format=None)),
        repeatable=True,
    )

    assert decision.allowed is False
    assert "execution shape is incomplete" in decision.reasons


def test_undeclared_repeatability_is_rejected():
    decision = evaluate_reuse(_observation(), planned_key=_key())

    assert decision.allowed is False
    assert any("not declared repeatable" in reason for reason in decision.reasons)


def test_incomplete_execution_is_rejected():
    decision = evaluate_reuse(
        _observation(complete=False),
        planned_key=_key(),
        repeatable=True,
    )

    assert decision.allowed is False
    assert any("complete execution" in reason for reason in decision.reasons)


def test_partial_coverage_is_rejected():
    decision = evaluate_reuse(
        _observation(coverage=0.8),
        planned_key=_key(),
        repeatable=True,
    )

    assert decision.allowed is False
    assert any("coverage" in reason for reason in decision.reasons)


def test_high_dispersion_is_rejected():
    decision = evaluate_reuse(
        _observation(relative_spread=DEFAULT_MAX_RELATIVE_SPREAD + 0.01),
        planned_key=_key(),
        repeatable=True,
    )

    assert decision.allowed is False
    assert any("relative spread" in reason for reason in decision.reasons)
    assert decision.confidence < ConfidenceLevel.LOW.value


def test_unknown_dispersion_is_rejected():
    decision = evaluate_reuse(
        _observation(relative_spread=None),
        planned_key=_key(),
        repeatable=True,
    )

    assert decision.allowed is False
    assert decision.confidence == ConfidenceLevel.UNKNOWN.value
    assert any("dispersion is unknown" in reason for reason in decision.reasons)


def test_single_sample_is_capped_at_medium():
    decision = evaluate_reuse(
        _observation(sample_count=1, relative_spread=0.0),
        planned_key=_key(),
        repeatable=True,
    )

    assert decision.allowed is True
    assert decision.confidence == MEDIUM


def test_low_confidence_can_be_gated_out():
    decision = evaluate_reuse(
        _observation(sample_count=1, relative_spread=0.09),
        planned_key=_key(),
        repeatable=True,
        min_confidence=ConfidenceLevel.HIGH.value,
    )

    assert decision.allowed is False
    assert any("confidence" in reason for reason in decision.reasons)


def test_fingerprint_covers_identity_and_shape():
    assert _key().fingerprint() == _key().fingerprint()
    assert _key().fingerprint() != _key(shape=_shape(batch_size=2048)).fingerprint()
    assert _key().fingerprint() != _key(udf_version="udf-v3").fingerprint()


def test_shape_diff_is_human_readable():
    assert _shape().diff(_shape()) == ()
    differences = _shape().diff(_shape(batch_size=1, execution_mode="actor_pool"))
    assert differences == (
        "batch_size: 1024 != 1",
        "execution_mode: 'task' != 'actor_pool'",
    )
