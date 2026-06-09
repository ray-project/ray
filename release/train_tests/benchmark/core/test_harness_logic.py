"""Torch-free unit tests for the benchmark harness's own logic.

Covers config parsing/overrides and the FLOPs/MFU math — the parts that are
easy to get wrong and don't need a GPU. The framework adapters and launchers
require torch/ray/deepspeed and are exercised on a GPU cluster instead.

Run: python -m pytest core/test_harness_logic.py
"""

import os

import pytest

from core.experiment_config import load_experiment
from core.metrics import (
    get_gpu_peak_flops,
    transformer_flops_per_token,
)

_EXPERIMENTS = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "experiments"
)


def test_load_qwen_experiment():
    cfg = load_experiment(os.path.join(_EXPERIMENTS, "qwen3_06b_deepspeed.yaml"))
    assert cfg.name == "qwen3_06b_deepspeed"
    assert cfg.model.adapter == "deepspeed"
    assert cfg.model.parallelism["zero_stage"] == 2
    assert cfg.data.seq_len == 2048
    assert cfg.training.num_steps == 200
    assert cfg.launcher == "ray"


def test_load_smoke_experiment():
    cfg = load_experiment(os.path.join(_EXPERIMENTS, "qwen3_06b_deepspeed_smoke.yaml"))
    assert cfg.data.dataset == "synthetic"
    assert cfg.num_workers == 1


def test_overrides_apply():
    cfg = load_experiment(
        os.path.join(_EXPERIMENTS, "qwen3_06b_deepspeed.yaml"),
        overrides=["training.num_steps=20", "data.dataset=synthetic", "num_workers=4"],
    )
    assert cfg.training.num_steps == 20
    assert cfg.data.dataset == "synthetic"
    assert cfg.num_workers == 4


def test_unknown_key_rejected(tmp_path):
    bad = tmp_path / "bad.yaml"
    bad.write_text(
        "name: bad\nmodel:\n  adapter: deepspeed\n  name: x\n  bogus: 1\n"
        "training:\n  num_steps: 1\n"
    )
    with pytest.raises(ValueError, match="bogus"):
        load_experiment(str(bad))


def test_requires_steps_or_epochs(tmp_path):
    bad = tmp_path / "bad.yaml"
    bad.write_text("name: bad\nmodel:\n  adapter: deepspeed\n  name: x\n")
    with pytest.raises(ValueError, match="num_steps"):
        load_experiment(str(bad))


def test_flops_per_token_matches_6n_approx():
    # For a model where the attention term is negligible, train FLOPs/token
    # should be ~6 * num_params.
    num_params = 600_000_000
    flops = transformer_flops_per_token(
        num_params=num_params, num_layers=28, hidden_size=1024, seq_len=2048
    )
    # 6N term dominates; attention term = 12 * 28 * 1024 * 2048.
    expected_6n = 6 * num_params
    attn = 12 * 28 * 1024 * 2048
    assert flops == expected_6n + attn
    assert flops > expected_6n


def test_gpu_peak_flops_lookup():
    assert get_gpu_peak_flops("NVIDIA A100-SXM4-40GB", "bf16") == 312e12
    assert get_gpu_peak_flops("Tesla T4", "fp16") == 65e12
    # Unknown device returns None rather than guessing.
    assert get_gpu_peak_flops("Some Future GPU", "bf16") is None
    # Known device, unsupported precision -> None.
    assert get_gpu_peak_flops("Tesla T4", "bf16") is None


def test_mfu_sanity():
    # An A100 at 50% MFU in bf16 should achieve ~156 TFLOP/s.
    peak = get_gpu_peak_flops("A100", "bf16")
    achieved = 0.5 * peak
    assert abs(achieved / peak - 0.5) < 1e-9
    assert achieved == pytest.approx(156e12)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main([__file__, "-v"]))
