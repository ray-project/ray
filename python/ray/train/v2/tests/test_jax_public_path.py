import os
import subprocess
import sys

import pytest

import ray.train.jax
import ray.train.v2.jax
from ray.train import ScalingConfig


def test_public_path_is_alias_of_v2_path():
    assert ray.train.jax.JaxTrainer is ray.train.v2.jax.JaxTrainer
    assert ray.train.jax.JaxConfig is ray.train.v2.jax.JaxConfig
    assert set(ray.train.jax.__all__) == set(ray.train.v2.jax.__all__)


def test_public_path_constructs_trainer():
    from ray.train.jax import JaxConfig, JaxTrainer

    trainer = JaxTrainer(
        lambda: None,
        jax_config=JaxConfig(use_tpu=False),
        scaling_config=ScalingConfig(num_workers=1),
    )
    assert isinstance(trainer.backend_config, JaxConfig)


@pytest.mark.parametrize("module", ["ray.train.jax", "ray.train.v2.jax"])
@pytest.mark.parametrize("v2_enabled", ["0", "1"])
def test_import_emits_no_warning(module, v2_enabled):
    """Both paths import without a warning, with Train V2 on or off.

    Runs in a fresh interpreter so the import isn't already cached. `ray` is
    imported first so that only warnings raised by importing `module` count.
    """
    code = (
        "import warnings\n"
        "import ray.train\n"
        "with warnings.catch_warnings(record=True) as caught:\n"
        "    warnings.simplefilter('always')\n"
        f"    import {module}\n"
        f"    {module}.JaxTrainer, {module}.JaxConfig\n"
        "assert not caught, [str(w.message) for w in caught]\n"
    )
    env = dict(os.environ, RAY_TRAIN_V2_ENABLED=v2_enabled)
    result = subprocess.run(
        [sys.executable, "-c", code], env=env, capture_output=True, text=True
    )
    assert result.returncode == 0, result.stderr


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
