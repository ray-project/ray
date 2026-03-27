import numpy as np
import pytest
from unittest.mock import MagicMock

from ray.air.integrations.trackio import (
    TrackioLoggerCallback,
    setup_trackio,
)


# -------------------------
# Mock Objects
# -------------------------

class MockTrial:
    def __init__(self):
        self.config = {"lr": 0.01, "callbacks": "remove_me"}
        self.experiment_dir_name = "exp_group"
        self.checkpoint = None

    def __str__(self):
        return "mock_trial"


class MockRun:
    def __init__(self):
        self.logged = []
        self.finished = False

    def log(self, metrics, step=None):
        self.logged.append((metrics, step))

    def finish(self):
        self.finished = True


# -------------------------
# Fixtures
# -------------------------

@pytest.fixture
def mock_trackio(mocker):
    mock_run = MockRun()

    mock_init = mocker.patch(
        "ray.air.integrations.trackio.trackio.init",
        return_value=mock_run,
    )

    return mock_init, mock_run


# -------------------------
# Tests: TrackioLoggerCallback
# -------------------------

def test_log_trial_start_initializes_run(mock_trackio):
    mock_init, _ = mock_trackio

    cb = TrackioLoggerCallback(project="test_project")
    trial = MockTrial()

    cb.log_trial_start(trial)

    assert trial in cb._trial_runs
    mock_init.assert_called_once()

    _, kwargs = mock_init.call_args
    assert kwargs["project"] == "test_project"
    assert kwargs["name"] == "mock_trial"
    assert kwargs["group"] == "exp_group"

    # callbacks removed
    assert "callbacks" not in kwargs["config"]


def test_log_trial_result_basic_metrics(mock_trackio):
    _, mock_run = mock_trackio

    cb = TrackioLoggerCallback(project="test")
    trial = MockTrial()

    cb.log_trial_start(trial)

    result = {
        "loss": 0.5,
        "accuracy": 0.9,
        "training_iteration": 3,
    }

    cb.log_trial_result(1, trial, result)

    assert len(mock_run.logged) == 1
    metrics, step = mock_run.logged[0]

    assert metrics == {"loss": 0.5, "accuracy": 0.9}
    assert step == 3


def test_flatten_dict_logging(mock_trackio):
    _, mock_run = mock_trackio

    cb = TrackioLoggerCallback(project="test")
    trial = MockTrial()

    cb.log_trial_start(trial)

    result = {
        "metrics": {
            "loss": 0.1,
            "acc": 0.95,
        }
    }

    cb.log_trial_result(1, trial, result)

    metrics, _ = mock_run.logged[0]

    assert metrics["metrics/loss"] == 0.1
    assert metrics["metrics/acc"] == 0.95


def test_numpy_conversion_and_filtering(mock_trackio):
    _, mock_run = mock_trackio

    cb = TrackioLoggerCallback(project="test")
    trial = MockTrial()

    cb.log_trial_start(trial)

    result = {
        "loss": np.float32(0.25),
        "arr": np.array([1, 2, 3]),  # dropped
    }

    cb.log_trial_result(1, trial, result)

    metrics, _ = mock_run.logged[0]

    assert metrics["loss"] == 0.25
    assert "arr" not in metrics


def test_warning_logged_once_per_key(mock_trackio, mocker):
    mocker.patch("ray.air.integrations.trackio.logger.warning")

    cb = TrackioLoggerCallback(project="test")
    trial = MockTrial()

    cb.log_trial_start(trial)

    result = {"bad_metric": "string"}

    cb.log_trial_result(1, trial, result)
    cb.log_trial_result(2, trial, result)

    logger_mock = pytest.importorskip(
        "ray.air.integrations.trackio"
    ).logger.warning

    assert logger_mock.call_count == 1


def test_excludes_filtering(mock_trackio):
    _, mock_run = mock_trackio

    cb = TrackioLoggerCallback(
        project="test",
        excludes=["loss"],
    )

    trial = MockTrial()
    cb.log_trial_start(trial)

    result = {
        "loss": 0.5,
        "accuracy": 0.8,
        "done": True,
    }

    cb.log_trial_result(1, trial, result)

    metrics, _ = mock_run.logged[0]

    assert "loss" not in metrics
    assert "done" not in metrics
    assert metrics["accuracy"] == 0.8


def test_lazy_initialization_on_result(mock_trackio):
    mock_init, _ = mock_trackio

    cb = TrackioLoggerCallback(project="test")
    trial = MockTrial()

    cb.log_trial_result(1, trial, {"loss": 1.0})

    assert mock_init.called
    assert trial in cb._trial_runs


def test_log_trial_end_finishes_run(mock_trackio):
    _, mock_run = mock_trackio

    cb = TrackioLoggerCallback(project="test")
    trial = MockTrial()

    cb.log_trial_start(trial)
    cb.log_trial_end(trial)

    assert mock_run.finished is True
    assert trial not in cb._trial_runs


def test_on_experiment_end_cleans_all_runs(mock_trackio):
    _, mock_run = mock_trackio

    cb = TrackioLoggerCallback(project="test")
    trial = MockTrial()

    cb.log_trial_start(trial)

    cb.on_experiment_end([])

    assert mock_run.finished is True
    assert cb._trial_runs == {}


# -------------------------
# Tests: setup_trackio
# -------------------------

def test_setup_trackio_rank_zero_only_blocks_nonzero(mocker):
    mocker.patch("ray.air.integrations.trackio.trackio.init")

    session = MagicMock()
    session.world_rank = 1
    session.trial_name = "trial"
    session.experiment_name = "exp"

    mocker.patch(
        "ray.air.integrations.trackio.get_session",
        return_value=session,
    )

    run = setup_trackio(rank_zero_only=True)

    assert run is None


def test_setup_trackio_initializes_on_rank_zero(mocker):
    mock_run = MagicMock()

    mock_init = mocker.patch(
        "ray.air.integrations.trackio.trackio.init",
        return_value=mock_run,
    )

    session = MagicMock()
    session.world_rank = 0
    session.trial_name = "trial_name"
    session.experiment_name = "exp_name"

    mocker.patch(
        "ray.air.integrations.trackio.get_session",
        return_value=session,
    )

    run = setup_trackio(config={"lr": 0.1}, project="proj")

    assert run == mock_run

    _, kwargs = mock_init.call_args
    assert kwargs["project"] == "proj"
    assert kwargs["name"] == "trial_name"
    assert kwargs["group"] == "exp_name"
    assert kwargs["config"]["lr"] == 0.1