"""Torch-free unit tests for the benchmark harness's own logic.

Covers config parsing/overrides and the FLOPs/MFU math — the parts that are
easy to get wrong and don't need a GPU. The framework adapters and launchers
require torch/ray/deepspeed and are exercised on a GPU cluster instead.

Run: python -m pytest core/test_harness_logic.py
"""

import os

import pytest

from core.experiment_config import load_experiment
from core.launchers.torchrun_ray_launcher import assign_topology
from core.metrics import (
    FlopsSpec,
    _physical_gpu_index,
    flops_per_token,
    get_gpu_peak_bandwidth_gbps,
    get_gpu_peak_flops,
)

_EXPERIMENTS = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "experiments"
)


def test_load_qwen_experiment():
    cfg = load_experiment(os.path.join(_EXPERIMENTS, "qwen3_06b_deepspeed.yaml"))
    assert cfg.name == "qwen3_06b_deepspeed"
    assert cfg.model.adapter == "deepspeed"
    assert cfg.model.parallelism["zero_stage"] == 1
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


def test_flops_per_token_dense_quadratic():
    # Dense: 6N param term + 12*L*d*s quadratic-attention term.
    num_params = 600_000_000
    flops = flops_per_token(
        FlopsSpec(
            active_params=num_params, num_layers=28, hidden_size=1024, seq_len=2048
        )
    )
    expected_6n = 6 * num_params
    attn = 12 * 28 * 1024 * 2048
    assert flops == expected_6n + attn


def test_flops_per_token_moe_uses_active_params():
    # MoE charges only active params in the 6N term; total experts never enter.
    spec = FlopsSpec(
        active_params=3_000_000_000,  # 3B active of a 35B model
        num_layers=40,
        hidden_size=2048,
        seq_len=4096,
        attention="linear",  # Gated DeltaNet -> no quadratic term
    )
    flops = flops_per_token(spec)
    # Linear attention omits the attention term -> pure 6 * active_params.
    assert flops == 6 * 3_000_000_000


def test_flops_per_token_linear_vs_quadratic():
    base = dict(active_params=10**9, num_layers=10, hidden_size=1024, seq_len=2048)
    quad = flops_per_token(FlopsSpec(attention="quadratic", **base))
    lin = flops_per_token(FlopsSpec(attention="linear", **base))
    none = flops_per_token(FlopsSpec(attention="none", **base))
    assert quad > lin == none == 6 * 10**9


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


def test_gpu_peak_bandwidth_lookup():
    assert get_gpu_peak_bandwidth_gbps("NVIDIA A10G") == 600.0
    # Both A100 variants map to the conservative 40GB value (substring can't
    # separate 40/80GB from the SXM name).
    assert get_gpu_peak_bandwidth_gbps("NVIDIA A100-SXM4-40GB") == 1555.0
    assert get_gpu_peak_bandwidth_gbps("NVIDIA A100-SXM4-80GB") == 1555.0
    assert get_gpu_peak_bandwidth_gbps("Some Future GPU") is None


def test_assign_topology_single_node():
    # 8 workers all on one node -> ranks 0-7, all node_rank 0, local 0-7.
    topo = assign_topology(["10.0.0.1"] * 8)
    assert [t["rank"] for t in topo] == list(range(8))
    assert all(t["node_rank"] == 0 for t in topo)
    assert [t["local_rank"] for t in topo] == list(range(8))
    assert all(t["local_world_size"] == 8 for t in topo)


def test_assign_topology_multi_node():
    # 2 nodes x 2 GPUs, interleaved discovery order.
    topo = assign_topology(["a", "b", "a", "b"])
    assert [t["node_rank"] for t in topo] == [0, 1, 0, 1]
    assert [t["local_rank"] for t in topo] == [0, 0, 1, 1]
    assert all(t["local_world_size"] == 2 for t in topo)


def test_expand_axes_cartesian():
    from sweep import cell_name, expand_axes

    cells = expand_axes(
        {"data.seq_len": ["1024", "2048"], "data.batch_size": ["1", "2"]}
    )
    assert len(cells) == 4
    assert {"data.seq_len": "1024", "data.batch_size": "2"} in cells
    assert cell_name("qwen", cells[0]) == "qwen__seq_len1024_batch_size1"


def test_megatron_view_defaults_parallelism():
    # DeepSpeed rows have no TP/PP/CP/EP -> Megatron view shows 1 (VP -> "-").
    from collect import _MEGATRON_COLUMNS, _format_cell

    row = {"config/model": "Qwen/Qwen3-0.6B", "world_size": 8}
    by_header = {c[0]: _format_cell(row, c) for c in _MEGATRON_COLUMNS}
    assert by_header["TP"] == "1"
    assert by_header["PP"] == "1"
    assert by_header["EP"] == "1"
    assert by_header["VP"] == "-"
    assert by_header["#-GPUs"] == "8"


def test_physical_gpu_index_maps_via_cvd(monkeypatch):
    # CVD="5,3": logical cuda:0 -> physical 5, cuda:1 -> physical 3.
    monkeypatch.setenv("CUDA_VISIBLE_DEVICES", "5,3")
    assert _physical_gpu_index(0) == 5
    assert _physical_gpu_index(1) == 3
    # No CVD: identity.
    monkeypatch.delenv("CUDA_VISIBLE_DEVICES", raising=False)
    assert _physical_gpu_index(2) == 2


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main([__file__, "-v"]))
