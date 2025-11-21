import sys

import pytest

from ray.llm._internal.batch.stages.configs import (
    ChatTemplateStageConfig,
    PrepareImageStageConfig,
    TokenizerStageConfig,
    resolve_stage_config,
)


def test_resolve_dict_to_config():
    """Dict → parsed StageConfig with all fields."""
    stage_cfg = resolve_stage_config(
        {"enabled": True, "batch_size": 128, "concurrency": 4},
        PrepareImageStageConfig,
    )
    assert isinstance(stage_cfg, PrepareImageStageConfig)
    assert stage_cfg.enabled is True
    assert stage_cfg.batch_size == 128
    assert stage_cfg.concurrency == 4


def test_resolve_typed_config_copied():
    """Typed config creates copy (doesn't mutate input)."""
    original = ChatTemplateStageConfig(
        enabled=True, batch_size=64, model_source="model1"
    )
    processor_defaults = {"batch_size": 128, "model_source": "model2"}

    # Resolve with first processor defaults
    resolved1 = resolve_stage_config(
        original, ChatTemplateStageConfig, processor_defaults
    )
    assert resolved1.batch_size == 64  # Explicit value preserved
    assert resolved1.model_source == "model1"  # Explicit value preserved

    # Resolve same original with different processor defaults
    processor_defaults2 = {"batch_size": 256, "model_source": "model3"}
    resolved2 = resolve_stage_config(
        original, ChatTemplateStageConfig, processor_defaults2
    )
    assert resolved2.batch_size == 64  # Still preserved
    assert resolved2.model_source == "model1"  # Still preserved

    # Original unchanged
    assert original.batch_size == 64
    assert original.model_source == "model1"


def test_resolve_merges_processor_defaults():
    """Defaults merged when None."""
    stage_cfg = resolve_stage_config(
        {"enabled": True},
        TokenizerStageConfig,
        processor_defaults={
            "batch_size": 128,
            "concurrency": 4,
            "model_source": "test-model",
        },
    )
    assert stage_cfg.batch_size == 128
    assert stage_cfg.concurrency == 4
    assert stage_cfg.model_source == "test-model"


def test_resolve_preserves_explicit_overrides():
    """Explicit values not overridden by defaults."""
    stage_cfg = resolve_stage_config(
        {"enabled": True, "batch_size": 64, "concurrency": 2},
        PrepareImageStageConfig,
        processor_defaults={"batch_size": 128, "concurrency": 4},
    )
    assert stage_cfg.batch_size == 64  # Explicit override preserved
    assert stage_cfg.concurrency == 2  # Explicit override preserved


def test_resolve_model_source_fallback():
    """model_source field uses processor default when None."""
    stage_cfg = resolve_stage_config(
        {"enabled": True},
        TokenizerStageConfig,
        processor_defaults={"model_source": "default-model"},
    )
    assert stage_cfg.model_source == "default-model"


def test_resolve_bool_true():
    """Bool True → enabled StageConfig."""
    stage_cfg = resolve_stage_config(True, PrepareImageStageConfig)
    assert isinstance(stage_cfg, PrepareImageStageConfig)
    assert stage_cfg.enabled is True


def test_resolve_bool_false():
    """Bool False → disabled StageConfig."""
    stage_cfg = resolve_stage_config(False, PrepareImageStageConfig)
    assert isinstance(stage_cfg, PrepareImageStageConfig)
    assert stage_cfg.enabled is False


def test_resolve_runtime_env_replacement():
    """Stage runtime_env replaces processor (not merged)."""
    stage_cfg = resolve_stage_config(
        {"enabled": True, "runtime_env": {"env_vars": {"STAGE_VAR": "stage_value"}}},
        PrepareImageStageConfig,
        processor_defaults={"runtime_env": {"env_vars": {"PROC_VAR": "proc_value"}}},
    )
    # Stage runtime_env completely replaces processor runtime_env
    assert stage_cfg.runtime_env == {"env_vars": {"STAGE_VAR": "stage_value"}}


def test_resolve_same_config_reusable():
    """Same StageConfig instance can be resolved with different processor defaults without mutation."""
    original = ChatTemplateStageConfig(enabled=True, batch_size=None, model_source=None)
    processor_defaults1 = {"batch_size": 128, "model_source": "model1"}
    processor_defaults2 = {"batch_size": 256, "model_source": "model2"}

    resolved1 = resolve_stage_config(
        original, ChatTemplateStageConfig, processor_defaults1
    )
    resolved2 = resolve_stage_config(
        original, ChatTemplateStageConfig, processor_defaults2
    )

    # Each resolution gets its own defaults
    assert resolved1.batch_size == 128
    assert resolved1.model_source == "model1"
    assert resolved2.batch_size == 256
    assert resolved2.model_source == "model2"

    # Original unchanged (still None)
    assert original.batch_size is None
    assert original.model_source is None


def test_resolve_unsupported_type():
    """Unsupported types raise TypeError."""
    with pytest.raises(TypeError, match="Unsupported type for stage config"):
        resolve_stage_config(None, PrepareImageStageConfig)

    with pytest.raises(TypeError, match="Unsupported type for stage config"):
        resolve_stage_config(123, PrepareImageStageConfig)


def test_resolve_stage_without_model_source():
    """Stages without model_source field don't get it from defaults."""
    stage_cfg = resolve_stage_config(
        {"enabled": True},
        PrepareImageStageConfig,
        processor_defaults={"model_source": "test-model"},
    )
    # PrepareImageStageConfig doesn't have model_source field
    assert not hasattr(stage_cfg, "model_source")


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
