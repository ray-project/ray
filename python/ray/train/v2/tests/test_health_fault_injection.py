"""The injection harness itself: does it issue the calls the vendors expect?

The DCGM tests assert we build exactly the command NVSentinel's own
``demos/local-fault-injection-demo`` issues, so a change on either side shows
up here rather than in a release test that takes ten minutes to fail.
"""
import os
import sys
from unittest import mock

import pytest

from ray.train.v2._internal.execution.health.report import reset, snapshot
from ray.train.v2._internal.execution.health.testing import dcgm, nvrx, symptoms


# ----------------------------------------------------------------------
# DCGM: the same call NVSentinel's demo makes
# ----------------------------------------------------------------------
def test_local_injection_matches_the_documented_dcgmi_call():
    inj = dcgm.DcgmInjector(dry_run=True)
    assert inj.command(dcgm.XID_ERRORS, 74) == [
        "dcgmi",
        "test",
        "--inject",
        "--gpuid",
        "0",
        "-f",
        "230",
        "-v",
        "74",
    ]


def test_in_cluster_injection_matches_the_nvsentinel_demo():
    """`kubectl exec -n gpu-operator <pod> -- dcgmi test --inject ... -f 84 -v 0`"""
    inj = dcgm.DcgmInjector(node="nvsentinel-demo-worker", dry_run=True)
    cmd = inj.command(dcgm.INFOROM_VALID, 0, pod="nvidia-dcgm-abcde")
    assert cmd == [
        "kubectl",
        "exec",
        "-n",
        "gpu-operator",
        "nvidia-dcgm-abcde",
        "--",
        "dcgmi",
        "test",
        "--inject",
        "--gpuid",
        "0",
        "-f",
        "84",
        "-v",
        "0",
    ]


def test_the_demos_fatal_fault_is_a_corrupt_inforom():
    inj = dcgm.DcgmInjector(node="w", dry_run=True)
    assert inj.inject_fatal_gpu_fault()[-3:] == ["84", "-v", "0"]


def test_field_ids_match_dcgm_fields_h():
    # Wrong ids fail silently -- DCGM accepts the injection and nothing fires.
    assert (dcgm.INFOROM_VALID, dcgm.GPU_TEMP, dcgm.XID_ERRORS) == (84, 150, 230)
    assert (dcgm.ECC_DBE_VOL_TOTAL, dcgm.PCIE_REPLAY_COUNTER) == (311, 202)


def test_observed_xids_are_the_ones_seen_in_production():
    # NVLink dominates; injecting a made-up distribution tests the wrong thing.
    assert dcgm.OBSERVED_XIDS.count(dcgm.XID_NVLINK_ERROR) == 2
    assert set(dcgm.OBSERVED_XIDS) == {74, 94, 79, 119}


def test_a_failed_injection_is_loud():
    inj = dcgm.DcgmInjector()
    with mock.patch("subprocess.run") as run:
        run.return_value = mock.Mock(returncode=1, stderr="no such field", stdout="")
        with pytest.raises(dcgm.DcgmInjectionFailed, match="no such field"):
            inj.inject(dcgm.XID_ERRORS, 74)


def test_a_missing_binary_says_so():
    inj = dcgm.DcgmInjector()
    with mock.patch("subprocess.run", side_effect=FileNotFoundError):
        with pytest.raises(dcgm.DcgmInjectionFailed, match="not found on PATH"):
            inj.inject(dcgm.XID_ERRORS, 74)


# ----------------------------------------------------------------------
# NVRx: refuse clearly rather than silently no-op
# ----------------------------------------------------------------------
def test_missing_nvrx_is_an_explicit_failure_by_default():
    with mock.patch.object(nvrx, "available", return_value=False):
        with pytest.raises(nvrx.NVRxUnavailable, match="nvidia-resiliency-ext"):
            nvrx.inject(nvrx.GPU_SLEEP)


def test_injection_can_be_made_optional():
    with mock.patch.object(nvrx, "available", return_value=False):
        assert nvrx.inject(nvrx.GPU_SLEEP, require=False) is False


def test_a_cuda_fault_on_a_cpu_runner_names_the_alternative():
    """The CPU-only runner is the common case, and the error has to be useful."""
    with mock.patch.object(nvrx, "available", return_value=True), mock.patch.object(
        nvrx, "_cuda_available", return_value=False
    ):
        with pytest.raises(nvrx.NVRxUnavailable, match="LOCK_GIL"):
            nvrx.inject(nvrx.GPU_SLEEP)


def test_the_silent_faults_need_no_gpu():
    # LOCK_GIL and SIGSTOP are the NodeMonitor's reason for existing, and both
    # run on any laptop.
    assert nvrx.SILENT_FAULTS == {"LOCK_GIL", "SIGSTOP"}
    assert not (nvrx.SILENT_FAULTS & nvrx.NEEDS_CUDA)


def test_empty_fault_list_is_rejected():
    with pytest.raises(ValueError):
        nvrx.inject([])


# ----------------------------------------------------------------------
# Symptoms: real effect, real reported numbers
# ----------------------------------------------------------------------
@pytest.fixture(autouse=True)
def _clean_report():
    reset()
    yield
    reset()
    os.environ.pop("RANK", None)


def test_a_straggler_actually_sleeps_and_reports_what_it_measured():
    os.environ["RANK"] = "4"
    fault = symptoms.Straggler(rank=4, slowdown=3.0)
    with fault.step(0):
        pass
    metrics = snapshot().metrics
    # The reported time is measured, not fabricated.
    assert metrics["step_time_s"] > 0
    assert metrics["compute_time_s"] == metrics["step_time_s"]


def test_an_untargeted_rank_is_untouched():
    os.environ["RANK"] = "0"
    fault = symptoms.Straggler(rank=4, slowdown=10.0)
    with fault.step(0):
        pass
    assert snapshot().metrics["step_time_s"] < 0.05


def test_a_dataload_straggler_keeps_compute_time_normal():
    """The discriminator that stops a starved shard evicting a healthy GPU."""
    os.environ["RANK"] = "4"
    fault = symptoms.Straggler(rank=4, slowdown=4.0, phase="dataload")
    with fault.step(0):
        pass
    metrics = snapshot().metrics
    assert metrics["compute_time_s"] < metrics["step_time_s"]


def test_a_wandering_straggler_targets_a_different_rank_each_step():
    os.environ["RANK"] = "2"
    fault = symptoms.WanderingStraggler(world_size=6, slowdown=2.0)
    assert fault.targets(2) is True
    assert fault.targets(3) is False
    assert fault.targets(8) is True  # 8 % 6 == 2


def test_a_numerical_fault_hits_one_rank():
    os.environ["RANK"] = "4"
    fault = symptoms.NumericalFault(rank=4)
    import math

    assert math.isnan(fault.observe(step=1))

    os.environ["RANK"] = "0"
    assert fault.observe(step=1) == 1.8


def test_the_global_control_hits_every_rank_on_one_step():
    os.environ["RANK"] = "0"
    fault = symptoms.NumericalFault(
        rank=4, everywhere=True, only_step=5, bad_value=900.0
    )
    assert fault.observe(step=4) == 1.8
    assert fault.observe(step=5) == 900.0
    assert fault.observe(step=6) == 1.8


# ----------------------------------------------------------------------
# The two GPU faults that need no NVRx
# ----------------------------------------------------------------------
def test_collective_desync_skips_only_the_target_rank_and_only_after_start():
    """The purest RAS input: one rank one op behind, everyone else blocked."""
    os.environ["RANK"] = "3"
    fault = symptoms.CollectiveDesync(rank=3, start_step=50)
    assert fault.maybe_skip(49) is False
    assert fault.maybe_skip(50) is True

    os.environ["RANK"] = "0"
    assert fault.maybe_skip(50) is False


def test_cuda_hang_only_fires_once():
    os.environ["RANK"] = "4"
    fault = symptoms.CudaHang(rank=4, start_step=0)
    with mock.patch.dict(sys.modules, {"torch": mock.MagicMock()}):
        sys.modules["torch"].cuda.is_available.return_value = True
        assert fault.fire(1) is True
        assert fault.fire(2) is False  # already wedged; do not queue another


def test_cuda_hang_refuses_without_a_gpu():
    os.environ["RANK"] = "4"
    fault = symptoms.CudaHang(rank=4)
    with mock.patch.dict(sys.modules, {"torch": mock.MagicMock()}):
        sys.modules["torch"].cuda.is_available.return_value = False
        with pytest.raises(RuntimeError, match="needs a CUDA device"):
            fault.fire(0)


def test_cuda_hang_ignores_other_ranks():
    os.environ["RANK"] = "0"
    assert symptoms.CudaHang(rank=4).fire(0) is False


def test_symptoms_report_through_the_real_api():
    os.environ["RANK"] = "4"
    symptoms.NumericalFault(rank=4, bad_value=99.0).observe(step=7)
    snap = snapshot()
    assert snap.metrics["grad_norm"] == 99.0
    assert snap.step == 7


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-x", __file__]))
