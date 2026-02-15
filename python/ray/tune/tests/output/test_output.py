import argparse
import sys
from unittest import mock

import pytest
from freezegun import freeze_time

from ray import tune
from ray.air.constants import TRAINING_ITERATION
from ray.tune.experiment.trial import Trial
from ray.tune.experimental.output import (
    AirVerbosity,
    TrainReporter,
    TuneTerminalReporter,
    _best_trial_str,
    _current_best_trial,
    _get_dict_as_table_data,
    _get_time_str,
    _get_trial_info,
    _get_trial_table_data,
    _get_trials_by_state,
    _infer_params,
    _infer_user_metrics,
    _max_len,
)
from ray.tune.utils.mock_trainable import MOCK_TRAINABLE_NAME

LAST_RESULT = {
    "custom_metrics": {},
    "episode_media": {},
    "info": {
        "learner": {
            "default_policy": {
                "allreduce_latency": 0.0,
                "grad_gnorm": 40.0,
                "cur_lr": 0.001,
                "total_loss": 93.35336303710938,
                "policy_loss": -18.39633560180664,
                "entropy": 0.5613694190979004,
                "entropy_coeff": 0.01,
                "var_gnorm": 23.452943801879883,
                "vf_loss": 223.5106201171875,
                "vf_explained_var": -0.0017577409744262695,
                "mean_IS": 0.9987365007400513,
                "var_IS": 0.0007558994111604989,
            },
        }
    },
    "sampler_results": {
        "episode_reward_max": 500.0,
        "episode_reward_min": 54.0,
        "episode_reward_mean": 214.45,
    },
    "episode_reward_max": 500.0,
    "episode_reward_min": 54.0,
    "episode_reward_mean": 214.45,
    "episode_len_mean": 214.45,
    "episodes_this_iter": 66,
    "timesteps_total": 33000,
}


@freeze_time("Mar 27th, 2023", auto_tick_seconds=15)
def test_get_time_str():
    base = 1679875200  # 2023-03-27 00:00:00

    assert _get_time_str(base, base) == ("2023-03-27 00:00:00", "0s")
    assert _get_time_str(base, base + 15) == ("2023-03-27 00:00:15", "15s")
    assert _get_time_str(base, base + 60) == ("2023-03-27 00:01:00", "1min 0s")
    assert _get_time_str(base, base + 65) == ("2023-03-27 00:01:05", "1min 5s")
    assert _get_time_str(base, base + 3600) == (
        "2023-03-27 01:00:00",
        "1hr 0min 0s",
    )
    assert _get_time_str(base, base + 3605) == (
        "2023-03-27 01:00:05",
        "1hr 0min 5s",
    )
    assert _get_time_str(base, base + 3660) == (
        "2023-03-27 01:01:00",
        "1hr 1min 0s",
    )
    assert _get_time_str(base, base + 86400) == (
        "2023-03-28 00:00:00",
        "1d 0hr 0min 0s",
    )


def test_get_trials_by_state():
    t1 = Trial(MOCK_TRAINABLE_NAME, stub=True)
    t1.set_status(Trial.RUNNING)
    t2 = Trial(MOCK_TRAINABLE_NAME, stub=True)
    t2.set_status(Trial.PENDING)
    trials = [t1, t2]
    assert _get_trials_by_state(trials) == {"RUNNING": [t1], "PENDING": [t2]}


def test_infer_user_metrics():
    t = Trial(MOCK_TRAINABLE_NAME, stub=True)
    t.run_metadata.last_result = LAST_RESULT
    result = [
        "episode_reward_max",
        "episode_reward_min",
        "episode_len_mean",
        "episodes_this_iter",
    ]
    assert _infer_user_metrics([t]) == result


def test_max_len():
    assert _max_len("long_metrics_name", max_len=5) == "...me"
    assert _max_len("long_metrics_name", max_len=10) == "...cs_name"
    assert _max_len("long_metrics_name", max_len=9, wrap=True) == "long_metr\nics_name"
    assert _max_len("long_metrics_name", max_len=8, wrap=True) == "..._metr\nics_name"


def test_current_best_trial():
    t1 = Trial(MOCK_TRAINABLE_NAME, stub=True)
    t2 = Trial(MOCK_TRAINABLE_NAME, stub=True)
    t1.run_metadata.last_result = {"metric": 2}
    t2.run_metadata.last_result = {"metric": 1}
    assert _current_best_trial([t1, t2], metric="metric", mode="min") == (t2, "metric")


def test_best_trial_str():
    t = Trial(MOCK_TRAINABLE_NAME, stub=True)
    t.trial_id = "18ae7_00005"
    t.run_metadata.last_result = {
        "loss": 0.5918508041056858,
        "config": {"train_loop_config": {"lr": 0.059253447253394785}},
    }
    assert (
        _best_trial_str(t, "loss")
        == "Current best trial: 18ae7_00005 with loss=0.5918508041056858"
        " and params={'train_loop_config': {'lr': 0.059253447253394785}}"
    )


def test_get_trial_info():
    t = Trial(MOCK_TRAINABLE_NAME, stub=True)
    t.trial_id = "af42b609"
    t.set_status(Trial.RUNNING)
    t.run_metadata.last_result = LAST_RESULT
    assert _get_trial_info(
        t,
        param_keys=[],
        metric_keys=[
            "episode_reward_mean",
            "episode_reward_max",
            "episode_reward_min",
            "episode_len_mean",
            "episodes_this_iter",
        ],
    ) == ["mock_trainable_af42b609", "RUNNING", 214.45, 500.0, 54.0, 214.45, 66]


def test_get_trial_table_data_less_than_20():
    trials = []
    for i in range(20):
        t = Trial(MOCK_TRAINABLE_NAME, stub=True)
        t.trial_id = str(i)
        t.set_status(Trial.RUNNING)
        t.run_metadata.last_result = {"episode_reward_mean": 100 + i}
        t.config = {"param": i}
        trials.append(t)
    table_data = _get_trial_table_data(trials, ["param"], ["episode_reward_mean"])
    header = table_data.header
    assert header == ["Trial name", "status", "param", "reward"]
    table_data = table_data.data
    assert len(table_data) == 1  # only the running category
    assert len(table_data[0].trial_infos) == 20
    assert not table_data[0].more_info


def test_get_trial_table_data_more_than_20():
    trials = []
    # total of 30 trials.
    for status in [Trial.RUNNING, Trial.TERMINATED, Trial.PENDING]:
        for i in range(10):
            t = Trial(MOCK_TRAINABLE_NAME, stub=True)
            t.trial_id = str(i)
            t.set_status(status)
            t.run_metadata.last_result = {"episode_reward_mean": 100 + i}
            t.config = {"param": i}
            trials.append(t)
    table_data = _get_trial_table_data(trials, ["param"], ["episode_reward_mean"])
    header = table_data.header
    assert header == ["Trial name", "status", "param", "reward"]
    table_data = table_data.data
    assert len(table_data) == 3  # only the running category
    for i in range(3):
        assert len(table_data[i].trial_infos) == 5
    assert table_data[0].more_info == "5 more RUNNING"
    assert table_data[1].more_info == "5 more TERMINATED"
    assert table_data[2].more_info == "5 more PENDING"


def test_infer_params():
    assert _infer_params({}) == []
    assert _infer_params({"some": "val"}) == []
    assert _infer_params({"some": "val", "param": tune.uniform(0, 1)}) == ["param"]
    assert _infer_params({"some": "val", "param": tune.grid_search([0, 1])}) == [
        "param"
    ]
    assert sorted(
        _infer_params(
            {
                "some": "val",
                "param": tune.grid_search([0, 1]),
                "other": tune.choice([0, 1]),
            }
        )
    ) == ["other", "param"]


def test_result_table_no_divison():
    data = _get_dict_as_table_data(
        {
            "b": 6,
            "a": 8,
            "x": 19.123123123,
            "c": 5,
            "ignore": 9,
            "nested_ignore": {"value": 5},
            "y": 20,
            "z": {"m": 4, "n": {"o": "p"}},
        },
        exclude={"ignore", "nested_ignore"},
    )

    assert data == [
        ["a", 8],
        ["b", 6],
        ["c", 5],
        ["x", "19.12312"],
        ["y", 20],
        ["z/m", 4],
        ["z/n/o", "p"],
    ]


def test_result_table_divison():
    data = _get_dict_as_table_data(
        {
            "b": 6,
            "a": 8,
            "x": 19.123123123,
            "c": 5,
            "ignore": 9,
            "nested_ignore": {"value": 5},
            "y": 20,
            "z": {"m": 4, "n": {"o": "p"}},
        },
        exclude={"ignore", "nested_ignore"},
        upper_keys={"x", "y", "z", "z/m", "z/n/o"},
    )

    assert data == [
        ["x", "19.12312"],
        ["y", 20],
        ["z/m", 4],
        ["z/n/o", "p"],
        ["a", 8],
        ["b", 6],
        ["c", 5],
    ]


def test_result_include():
    data = _get_dict_as_table_data(
        {
            "b": 6,
            "a": 8,
            "x": 19.123123123,
            "c": 5,
            "ignore": 9,
            "nested_ignore": {"value": 5},
            "y": 20,
            "z": {"m": 4, "n": {"o": "p"}},
        },
        include={"y", "z"},
        exclude={"z/n/o"},
    )

    assert data == [
        ["y", 20],
        ["z/m", 4],
    ]


def test_config_argparse():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bool-val", action="store_true", default=True)
    parser.add_argument("--foo", default="bar")
    args = parser.parse_args([])

    data = _get_dict_as_table_data({"parsed_args": args})
    assert data == [
        ["parsed_args/bool_val", True],
        ["parsed_args/foo", "bar"],
    ]


@pytest.mark.parametrize("progress_reporter_cls", [TrainReporter, TuneTerminalReporter])
def test_heartbeat_reset(progress_reporter_cls):
    """Test heartbeat functionality in train and tune.

    Tune prints a table every `heartbeat_freq` seconds.
    Train prints a heartbeat every `heartbeat_freq` seconds, but a result
    also resets the counter.
    """
    # Train heartbeats are only reporter in VERBOSE
    reporter = progress_reporter_cls(verbosity=AirVerbosity.VERBOSE)
    reporter._print_heartbeat = mock.MagicMock()

    with freeze_time() as frozen:
        reporter.print_heartbeat([])
        assert reporter._print_heartbeat.call_count == 1

        # Tick until heartbeat freq. Next call to print_heartbeat should trigger
        frozen.tick(reporter._heartbeat_freq)
        reporter.print_heartbeat([])
        assert reporter._print_heartbeat.call_count == 2

        # Not quite there, yet. This should not trigger a heartbeat.
        frozen.tick(reporter._heartbeat_freq // 2)
        reporter.print_heartbeat([])
        assert reporter._print_heartbeat.call_count == 2

        # Let's report a result. This will reset the heartbeat timer
        reporter.on_trial_result(
            0, [], Trial(MOCK_TRAINABLE_NAME, stub=True), {TRAINING_ITERATION: 1}
        )

        # Progress another half heartbeat. In Tune this triggers a heartbeat,
        # but in train the heartbeat is reset on trial result.
        frozen.tick(reporter._heartbeat_freq // 2 + 1)
        reporter.print_heartbeat([])

        if progress_reporter_cls == TrainReporter:
            # Thus, train shouldn't have reported
            assert reporter._print_heartbeat.call_count == 2
        elif progress_reporter_cls == TuneTerminalReporter:
            # But Tune should have.
            assert reporter._print_heartbeat.call_count == 3
        else:
            raise RuntimeError("Test faulty.")


def test_legacy_progress_reporter_adapter_delegates_setup():
    """Test that the adapter delegates setup() to the legacy reporter."""
    from ray.tune.experimental.output import LegacyProgressReporterAdapter

    legacy_reporter = mock.MagicMock()
    adapter = LegacyProgressReporterAdapter(
        legacy_reporter=legacy_reporter, verbosity=AirVerbosity.DEFAULT
    )

    adapter.setup(start_time=100.0, total_samples=10, metric="loss", mode="min")

    legacy_reporter.setup.assert_called_once_with(
        start_time=100.0, total_samples=10, metric="loss", mode="min"
    )


def test_legacy_progress_reporter_adapter_heartbeat():
    """Test that print_heartbeat delegates to should_report/report."""
    from ray.tune.experimental.output import LegacyProgressReporterAdapter

    legacy_reporter = mock.MagicMock()
    legacy_reporter.should_report.return_value = True
    adapter = LegacyProgressReporterAdapter(
        legacy_reporter=legacy_reporter, verbosity=AirVerbosity.DEFAULT
    )

    trials = [Trial(MOCK_TRAINABLE_NAME, stub=True)]
    adapter.print_heartbeat(trials, "resources_str", force=False)

    legacy_reporter.should_report.assert_called_once_with(trials, done=False)
    legacy_reporter.report.assert_called_once_with(trials, False, "resources_str")


def test_legacy_progress_reporter_adapter_heartbeat_skips_when_should_report_false():
    """Test that report() is not called when should_report returns False."""
    from ray.tune.experimental.output import LegacyProgressReporterAdapter

    legacy_reporter = mock.MagicMock()
    legacy_reporter.should_report.return_value = False
    adapter = LegacyProgressReporterAdapter(
        legacy_reporter=legacy_reporter, verbosity=AirVerbosity.DEFAULT
    )

    trials = [Trial(MOCK_TRAINABLE_NAME, stub=True)]
    adapter.print_heartbeat(trials, "resources_str", force=False)

    legacy_reporter.should_report.assert_called_once_with(trials, done=False)
    legacy_reporter.report.assert_not_called()


def test_legacy_progress_reporter_adapter_force_sets_done():
    """Test that force=True passes done=True to the legacy reporter."""
    from ray.tune.experimental.output import LegacyProgressReporterAdapter

    legacy_reporter = mock.MagicMock()
    legacy_reporter.should_report.return_value = True
    adapter = LegacyProgressReporterAdapter(
        legacy_reporter=legacy_reporter, verbosity=AirVerbosity.DEFAULT
    )

    trials = []
    adapter.print_heartbeat(trials, force=True)

    legacy_reporter.should_report.assert_called_once_with(trials, done=True)
    legacy_reporter.report.assert_called_once_with(trials, True)


def test_legacy_progress_reporter_adapter_callback_hooks_are_noop():
    """Test that callback hooks don't call the legacy reporter."""
    from ray.tune.experimental.output import LegacyProgressReporterAdapter

    legacy_reporter = mock.MagicMock()
    adapter = LegacyProgressReporterAdapter(
        legacy_reporter=legacy_reporter, verbosity=AirVerbosity.DEFAULT
    )

    trial = Trial(MOCK_TRAINABLE_NAME, stub=True)
    adapter.on_trial_result(0, [trial], trial, {TRAINING_ITERATION: 1})
    adapter.on_trial_complete(0, [trial], trial)
    adapter.on_trial_error(0, [trial], trial)
    adapter.on_trial_start(0, [trial], trial)

    legacy_reporter.report.assert_not_called()
    legacy_reporter.should_report.assert_not_called()


def test_create_default_callbacks_uses_adapter_for_legacy_reporter():
    """Test that _create_default_callbacks wraps a legacy reporter in an adapter."""
    from ray.tune.experimental.output import LegacyProgressReporterAdapter
    from ray.tune.utils.callback import _create_default_callbacks

    legacy_reporter = mock.MagicMock()
    callbacks = _create_default_callbacks(
        [],
        air_verbosity=AirVerbosity.DEFAULT,
        progress_reporter=legacy_reporter,
    )

    adapters = [c for c in callbacks if isinstance(c, LegacyProgressReporterAdapter)]
    assert len(adapters) == 1
    assert adapters[0]._legacy_reporter is legacy_reporter


def test_create_default_callbacks_uses_default_reporter_without_legacy():
    """Test that _create_default_callbacks uses the default reporter when no
    legacy reporter is provided."""
    from ray.tune.experimental.output import (
        LegacyProgressReporterAdapter,
        ProgressReporter,
    )
    from ray.tune.utils.callback import _create_default_callbacks

    callbacks = _create_default_callbacks(
        [],
        air_verbosity=AirVerbosity.DEFAULT,
    )

    air_reporters = [c for c in callbacks if isinstance(c, ProgressReporter)]
    assert len(air_reporters) == 1
    assert not isinstance(air_reporters[0], LegacyProgressReporterAdapter)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
