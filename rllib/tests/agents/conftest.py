"""Fixtures for use in agents tests.

The following fixtures are defined:

ray_env:
    Initializes ray during test function setup and shuts it down on teardown.
using_framework:
    Allows one to specify one or more frameworks (TensorFlow Graph Mode, TensorFlow
    Eager Mode, PyTorch) to run a test on.
trainer:
    Instantiates a trainer.
    Useful for writing generic tests that are common to multiple algorithms.
    For example, compilation tests (ensure that a trainer compiles on all frameworks),
    and convergence tests are common to all algorithms. These tests use the trainer
    fixture to parametrize the test to test multiple algorithms.
"""
import logging

import pytest
import ray
from _pytest.fixtures import FixtureRequest

from ray.rllib.agents.trainer import Trainer
from ray.rllib.agents.trainer_factory import (
    Algorithm,
    Framework,
    trainer_factory,
)

logger = logging.getLogger(__name__)


@pytest.fixture()
def ray_env(request: FixtureRequest) -> None:
    """Initializes ray and shuts it down after each test using this fixture.

    Usage: A test can be made to use the ray_env fixture in two ways:

        1. Pass `ray_env` as a parameter to the test

        >>> def test_foo(ray_env, *other_test_fixtures_and_params): ...

        2. Use the `pytest.mark.usefixtures` decorator

        >>> @pytest.mark.usefixtures("ray_env")
        ... def test_foo(*test_fixtures_and_params): ...

    Args:
        request (FixtureRequest): pytest fixture
            used for test teardown
    """

    def cleanup():
        """All teardown functionality goes in this function."""
        if ray.is_initialized():
            ray.shutdown()

    ray.init()
    request.addfinalizer(cleanup)


def _using_torch_framework_(config_overrides_: dict) -> None:
    try:
        import torch
    except ImportError:
        pytest.skip("`using_framework` fixture skipping torch (Not Installed)!")
    else:
        config_overrides_["framework"] = "torch"


def _using_eager_framework_(config_overrides_: dict) -> None:
    try:
        import tensorflow as tf
    except ImportError:
        pytest.skip(
            "`using_framework` fixture skipping eager (TensorFlow Not Installed)!"
        )
        return
    try:
        from tensorflow.python.eager.context import eager_mode
    except (ImportError, ModuleNotFoundError):
        eager_mode = None
    if not eager_mode:
        pytest.skip(
            "`using_framework` fixture skipping eager "
            + "(Could Not Import `eager_mode` from TensorFlow)!"
        )
        return
    with eager_mode():
        assert tf.executing_eagerly()
        config_overrides_["framework"] = "tfe"
        # TODO(Adi): Can this be just return instead of yield?
        yield


def _using_tensorflow_framework_(config_overrides_: dict) -> None:
    try:
        import tensorflow as tf
    except ImportError:
        pytest.skip(
            "`using_framework` fixture skipping tensorflow (TensorFlow Not Installed)!"
        )
        return
    assert not tf.executing_eagerly()
    config_overrides_["framework"] = "tf"


@pytest.fixture()
def using_framework(config_overrides: dict, framework: Framework) -> None:
    """Sets up test and trainer to use the specified framework.

    Usage: A test can be made to use a framework with this fixture in two ways:
        1. Pass `using_framework` as a parameter to the test along, and parameterize the
        test with `config_overrides` and `framework`

        >>> @pytest.mark.parametrize("config_overrides, framework", [{},
        Framework.Torch])
        ... def test_foo(config_overrides, framework, using_framework, *args): ...

        2. Use the `pytest.mark.usefixtures` decorator on the test along, and
        parameterize the test with `config_overrides` and `framework`

        >>> @pytest.mark.usefixtures("using_framework")
        ... @pytest.mark.parametrize("config_overrides, framework", [{},
        Framework.Torch])
        ... def test_foo(config_overrides, framework, *args): ...

    Args:
        config_overrides (dict): config overrides that will be passed to the trainer
        framework (Framework): framework to use
            Using Framework.Eager WILL automatically setup and enter an eager context
            Using Framework.TensorFlow WILL NOT enter a TensorFlow Session context. Use
            `tf_test_session` fixture for that.

    """
    logger.info("USING Framework {}".format(framework))
    if framework is Framework.Eager:
        yield _using_eager_framework_(config_overrides)
    elif framework is Framework.Torch:
        yield _using_torch_framework_(config_overrides)
    elif framework is Framework.TensorFlow:
        yield _using_tensorflow_framework_(config_overrides)
    else:
        raise NotImplementedError


@pytest.fixture()
def trainer(
    algorithm: Algorithm, config_overrides: dict, env: str, request: FixtureRequest
) -> Trainer:
    """Constructs a trainer using its default config updated with given config."""
    trainer = trainer_factory(
        algorithm=algorithm, config_overrides=config_overrides, env=env
    )

    def cleanup():
        try:
            trainer.stop()
        except Exception as e:
            logger.error(e)

    request.addfinalizer(cleanup)

    return trainer
