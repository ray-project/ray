"""Bounded diagnostics with in-memory declines; no malformed reader fixtures."""

import logging
from concurrent.futures import ThreadPoolExecutor

import pyarrow as pa
import pyarrow.fs as fs
import pytest

from ray.data._internal.datasource_v2.formats.parquet import footer_reader as module


@pytest.fixture
def reader():
    from ray.data._internal.datasource_v2.formats.parquet.listing_pruning import (
        HivePartitioningSnapshot,
        ListingPruningContext,
        ListingPruningHint,
    )
    from ray.data.expressions import col

    schema = pa.schema([("x", pa.int64())])
    context = ListingPruningContext(
        schema, schema, (("x", "int64"),), HivePartitioningSnapshot("", (), ())
    )
    hint = ListingPruningHint.from_residual(context, col("x") > 0)
    assert hint is not None
    value = module.FooterReader(
        fs.LocalFileSystem(), io_concurrency=1, listing_hint=hint
    )
    try:
        yield value
    finally:
        value.pool.shutdown(wait=True)


def decline(reader, monkeypatch, reason="unsupported_footer_metadata"):
    monkeypatch.setattr(module, "physical_schema_decline_reason", lambda *args: reason)
    baseline = [0, 1, 2]
    assert (
        reader._apply_listing_hint(None, None, baseline, "private/path")[0] is baseline
    )


def test_disabled_debug_does_not_lock_or_consume_report(reader, monkeypatch):
    lock_attempts = []

    class ForbiddenLock:
        def __enter__(self):
            lock_attempts.append(1)
            raise AssertionError("debug disabled must not lock")

        def __exit__(self, exc_type, exc_value, traceback):
            return False

    reader._listing_decline_lock = ForbiddenLock()
    # Negative sensitivity: a complete context-manager protocol must reach the
    # probe's __enter__, rather than fail before it and get swallowed by logging.
    with pytest.raises(AssertionError, match="debug disabled must not lock"):
        with reader._listing_decline_lock:
            pass
    assert lock_attempts == [1]
    lock_attempts.clear()
    monkeypatch.setattr(module.logger, "isEnabledFor", lambda level: False)
    monkeypatch.setattr(
        module.logger, "debug", lambda *a, **k: pytest.fail("unexpected log")
    )
    decline(reader, monkeypatch)
    assert reader._listing_decline_reported is False
    assert lock_attempts == []


def test_concurrent_declines_attempt_only_one_fixed_message(reader, monkeypatch):
    records = []
    monkeypatch.setattr(
        module.logger, "isEnabledFor", lambda level: level == logging.DEBUG
    )
    monkeypatch.setattr(module.logger, "debug", lambda *a, **k: records.append((a, k)))
    monkeypatch.setattr(
        module, "physical_schema_decline_reason", lambda metadata, _: metadata
    )
    baseline = [0, 1, 2]
    reasons = ["unsupported_footer_metadata", "unsupported_physical_column"] * 32
    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(
            pool.map(
                lambda reason: reader._apply_listing_hint(
                    None, reason, baseline, "private/path"
                ),
                reasons,
            )
        )
    assert all(result[0] is baseline and result[1] is False for result in results)
    assert len(records) == 1
    args, kwargs = records[0]
    assert args[1] in reasons
    assert "private" not in repr(records)
    assert kwargs == {}
    assert reader._listing_decline_reported


def test_handler_runs_outside_lock(reader, monkeypatch):
    lock_available = []

    def handler(*args, **kwargs):
        acquired = reader._listing_decline_lock.acquire(blocking=False)
        lock_available.append(acquired)
        if acquired:
            reader._listing_decline_lock.release()

    monkeypatch.setattr(module.logger, "isEnabledFor", lambda _: True)
    monkeypatch.setattr(module.logger, "debug", handler)
    decline(reader, monkeypatch)
    # Single-threaded so another footer thread cannot create a false failure.
    assert lock_available == [True]


@pytest.mark.parametrize("failure", ["level", "handler"])
def test_logging_failure_cannot_change_baseline(reader, monkeypatch, failure):
    attempts = []

    def broken(*args, **kwargs):
        attempts.append(1)
        raise RuntimeError("sensitive exception text")

    monkeypatch.setattr(
        module.logger, "isEnabledFor", broken if failure == "level" else lambda _: True
    )
    monkeypatch.setattr(module.logger, "debug", broken)
    decline(reader, monkeypatch)
    decline(reader, monkeypatch)
    assert len(attempts) == (2 if failure == "level" else 1)


def test_unknown_reason_cannot_leak(reader, monkeypatch):
    records = []
    monkeypatch.setattr(module.logger, "isEnabledFor", lambda _: True)
    monkeypatch.setattr(module.logger, "debug", lambda *a, **k: records.append((a, k)))
    decline(reader, monkeypatch, "secret/path/or/literal")
    assert records[0][0][1] == "optional_analysis_failed"
    assert "secret" not in repr(records)


def test_optional_exception_diagnostic_does_not_expose_exception(reader, monkeypatch):
    records = []
    monkeypatch.setattr(module.logger, "isEnabledFor", lambda _: True)
    monkeypatch.setattr(module.logger, "debug", lambda *a, **k: records.append((a, k)))

    def unavailable(_):
        raise RuntimeError("secret schema or data")

    monkeypatch.setattr(module, "logical_schema_signature", unavailable)
    baseline = [0, 1]
    assert (
        reader._apply_listing_hint(None, None, baseline, "private/path")[0] is baseline
    )
    assert records[0][0][1] == "optional_analysis_failed"
    assert "secret" not in repr(records)


@pytest.mark.parametrize("mode", ["no_hint", "excluded"])
def test_ordinary_early_returns_do_not_log(reader, monkeypatch, mode):
    monkeypatch.setattr(
        module.logger, "isEnabledFor", lambda _: pytest.fail("no probe")
    )
    baseline = [0] if mode == "singleton" else [0, 1]
    if mode == "no_hint":
        reader.listing_hint = None
    elif mode == "excluded":
        reader.excluded_read_unit_ids = {"already-done"}
    assert (
        reader._apply_listing_hint(None, None, baseline, "private/path")[0] is baseline
    )
    assert not reader._listing_decline_reported


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
