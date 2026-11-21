import io
import logging
import sys
import time
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from ray.tune.execution.insufficient_resources_manager import (
    _can_fulfill_no_autoscaler,
    _InsufficientResourcesManager,
    logger as insufficient_resources_logger,
)
from ray.tune.experiment import Trial
from ray.tune.search.variant_generator import format_vars
from ray.tune.utils.util import Tee, logger as util_logger, retry_fn


def test_format_vars():

    # Format brackets correctly
    assert (
        format_vars(
            {
                ("a", "b", "c"): 8.1234567,
                ("a", "b", "d"): [7, 8],
                ("a", "b", "e"): [[[3, 4]]],
            }
        )
        == "c=8.1235,d=7_8,e=3_4"
    )
    # Sorted by full keys, but only last key is reported
    assert (
        format_vars(
            {
                ("a", "c", "x"): [7, 8],
                ("a", "b", "x"): 8.1234567,
            }
        )
        == "x=8.1235,x=7_8"
    )
    # Filter out invalid chars. It's ok to have empty keys or values.
    assert (
        format_vars(
            {
                ("a  c?x",): " <;%$ok ",
                ("some",): " ",
            }
        )
        == "a_c_x=ok,some="
    )


def test_retry_fn_repeat(tmpdir):
    success = tmpdir / "success"
    marker = tmpdir / "marker"

    def _fail_once():
        if marker.exists():
            success.write_text(".", encoding="utf-8")
            return
        marker.write_text(".", encoding="utf-8")
        raise RuntimeError("Failing")

    assert not success.exists()
    assert not marker.exists()

    assert retry_fn(
        fn=_fail_once,
        exception_type=RuntimeError,
        sleep_time=0,
    )

    assert success.exists()
    assert marker.exists()


def test_retry_fn_timeout(tmpdir):
    success = tmpdir / "success"
    marker = tmpdir / "marker"

    def _fail_once():
        if not marker.exists():
            marker.write_text(".", encoding="utf-8")
            raise RuntimeError("Failing")
        time.sleep(5)
        success.write_text(".", encoding="utf-8")
        return

    assert not success.exists()
    assert not marker.exists()

    assert not retry_fn(
        fn=_fail_once, exception_type=RuntimeError, sleep_time=0, timeout=0.1
    )

    assert not success.exists()
    assert marker.exists()


def test_tee_recursion():
    f = io.StringIO()
    g = io.StringIO()

    tee = Tee(f, g)

    hdlr = logging.StreamHandler(tee)
    util_logger.addHandler(hdlr)

    util_logger.info("BEFORE")
    f.close()
    util_logger.info("AFTER")

    util_logger.removeHandler(hdlr)


def _make_fake_trial(status: str, cpus: float = 1, gpus: float = 0):
    return SimpleNamespace(
        status=status,
        placement_group_factory=SimpleNamespace(
            required_resources={"CPU": cpus, "GPU": gpus}
        ),
    )


def test_insufficient_resources_manager_reports_pending_trial_and_updated_cluster(
    monkeypatch,
):
    monkeypatch.setenv("TUNE_WARN_INSUFFICENT_RESOURCE_THRESHOLD_S", "0")
    terminated_trial = _make_fake_trial(Trial.TERMINATED, cpus=1, gpus=0)
    pending_trial = _make_fake_trial(Trial.PENDING, cpus=8, gpus=2)

    with patch("ray.cluster_resources", return_value={"CPU": 4.0, "GPU": 0.0}):
        assert not _can_fulfill_no_autoscaler(pending_trial)
        mgr = _InsufficientResourcesManager(for_train=False)
        with patch.object(insufficient_resources_logger, "warning") as warn_mock:
            mgr.on_no_available_trials([terminated_trial, pending_trial])
            mgr.on_no_available_trials([terminated_trial, pending_trial])
            time.sleep(0.01)
            mgr.on_no_available_trials([terminated_trial, pending_trial])
            warn_mock.assert_called_once()
            warning_msg = warn_mock.call_args[0][0]
            assert "8 CPUs and 2 GPUs" in warning_msg
            assert "4.0 CPUs and 0.0 GPUs" in warning_msg

    # Cluster resources updated (e.g., after ray.shutdown()/ray.init() or node addition)
    with patch("ray.cluster_resources", return_value={"CPU": 16.0, "GPU": 4.0}):
        assert _can_fulfill_no_autoscaler(pending_trial)


def test_insufficient_resources_manager_empty_or_non_pending_trials(monkeypatch):
    monkeypatch.setenv("TUNE_WARN_INSUFFICENT_RESOURCE_THRESHOLD_S", "0")
    terminated_trial = _make_fake_trial(Trial.TERMINATED, cpus=1, gpus=0)

    with patch("ray.cluster_resources", return_value={"CPU": 1.0, "GPU": 0.0}):
        mgr = _InsufficientResourcesManager(for_train=False)
        with patch.object(insufficient_resources_logger, "warning") as warn_mock:
            mgr.on_no_available_trials([])
            mgr.on_no_available_trials([])
            time.sleep(0.01)
            mgr.on_no_available_trials([])
            mgr.on_no_available_trials([terminated_trial])
            mgr.on_no_available_trials([terminated_trial])
            time.sleep(0.01)
            mgr.on_no_available_trials([terminated_trial])
            warn_mock.assert_not_called()


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
