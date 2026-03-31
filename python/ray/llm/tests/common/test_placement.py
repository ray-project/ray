"""Unit tests for ray.llm._internal.common.placement."""

import sys

import pytest
from pydantic import ValidationError

from ray.llm._internal.common.placement import BundleConfig, PlacementGroupConfig


def test_bundle_config_defaults_returns_float():
    b = BundleConfig()
    d = b.model_dump()
    assert d["CPU"] == 0.0 and d["GPU"] == 0.0

    b2 = BundleConfig(CPU=2, GPU=1)
    assert b2.model_dump() == {"CPU": 2.0, "GPU": 1.0}


def test_bundle_config_fractional_gpu():
    b = BundleConfig(GPU=0.5, CPU=1.0)
    assert b.model_dump()["GPU"] == 0.5


def test_bundle_config_extra_custom_resource():
    b = BundleConfig(CPU=0.0, GPU=0.0, TPU=4.0)
    d = b.model_dump()
    assert d["TPU"] == 4.0


def test_bundle_config_extra_resource_negative_rejected():
    with pytest.raises(ValueError, match="non-negative"):
        BundleConfig(TPU=-1.0)


def test_bundle_config_extra_resource_non_numeric_rejected():
    with pytest.raises(ValueError, match="must be a number"):
        BundleConfig(CPU=0.0, GPU=0.0, bad="x")


def test_placement_group_config_bundles_only():
    pg = PlacementGroupConfig(
        bundles=[BundleConfig(GPU=1.0, CPU=1.0)],
        strategy="PACK",
    )
    out = pg.model_dump()
    assert out["bundles"][0]["GPU"] == 1.0
    assert out["strategy"] == "PACK"


def test_placement_group_config_bundle_per_worker_only():
    pg = PlacementGroupConfig(
        bundle_per_worker=BundleConfig(GPU=1.0, CPU=2.0),
        strategy="SPREAD",
    )
    out = pg.model_dump()
    assert out["bundle_per_worker"]["GPU"] == 1.0
    assert out["strategy"] == "SPREAD"


def test_placement_group_config_rejects_neither_bundles_nor_per_worker():
    with pytest.raises(ValueError, match="either 'bundle_per_worker'"):
        PlacementGroupConfig(strategy="PACK")


def test_placement_group_config_rejects_both_bundles_and_per_worker():
    with pytest.raises(ValueError, match="Cannot specify both"):
        PlacementGroupConfig(
            bundles=[BundleConfig(GPU=1.0)],
            bundle_per_worker=BundleConfig(GPU=1.0),
        )


def test_placement_group_config_invalid_strategy_rejected():
    with pytest.raises(ValidationError):
        PlacementGroupConfig.model_validate(
            {
                "bundles": [{"GPU": 1.0, "CPU": 0.0}],
                "strategy": "INVALID",
            }
        )


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
