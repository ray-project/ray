import pytest
import sys

import time
from freezegun import freeze_time

from ray.tune.experimental.output import (
    _get_time_str,
    _get_trials_by_state,
    _get_trial_info,
    _infer_user_metrics,
    _max_len,
    _current_best_trial,
    _best_trial_str,
    _get_trial_table_data,
    _get_dict_as_table_data,
)
from ray.tune.experiment.trial import Trial


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
    result = _get_time_str(time.time(), time.time())
    assert result == ("2023-03-27 00:00:15", "00:00:15.00")


def test_get_trials_by_state():
    t1 = Trial("__fake")
    t1.set_status(Trial.RUNNING)
    t2 = Trial("__fake")
    t2.set_status(Trial.PENDING)
    trials = [t1, t2]
    assert _get_trials_by_state(trials) == {"RUNNING": [t1], "PENDING": [t2]}


def test_infer_user_metrics():
    t = Trial("__fake")
    t.last_result = LAST_RESULT
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
    t1 = Trial("__fake")
    t2 = Trial("__fake")
    t1.last_result = {"metric": 2}
    t2.last_result = {"metric": 1}
    assert _current_best_trial([t1, t2], metric="metric", mode="min") == (t2, "metric")


def test_best_trial_str():
    t = Trial("__fake")
    t.trial_id = "18ae7_00005"
    t.last_result = {
        "loss": 0.5918508041056858,
        "config": {"train_loop_config": {"lr": 0.059253447253394785}},
    }
    assert (
        _best_trial_str(t, "loss")
        == "Current best trial: 18ae7_00005 with loss=0.5918508041056858"
        " and params={'train_loop_config': {'lr': 0.059253447253394785}}"
    )


def test_get_trial_info():
    t = Trial("__fake")
    t.trial_id = "af42b609"
    t.set_status(Trial.RUNNING)
    t.last_result = LAST_RESULT
    assert _get_trial_info(
        t,
        [
            "episode_reward_mean",
            "episode_reward_max",
            "episode_reward_min",
            "episode_len_mean",
            "episodes_this_iter",
        ],
    ) == ["__fake_af42b609", "RUNNING", 214.45, 500.0, 54.0, 214.45, 66]


def test_get_trial_table_data_less_than_20():
    trials = []
    for i in range(20):
        t = Trial("__fake")
        t.trial_id = str(i)
        t.set_status(Trial.RUNNING)
        t.last_result = {"episode_reward_mean": 100 + i}
        trials.append(t)
    table_data = _get_trial_table_data(trials, ["episode_reward_mean"])
    header = table_data.header
    assert header == ["Trial name", "status", "reward"]
    table_data = table_data.data
    assert len(table_data) == 1  # only the running category
    assert len(table_data[0].trial_infos) == 20
    assert not table_data[0].more_info


def test_get_trial_table_data_more_than_20():
    trials = []
    # total of 30 trials.
    for status in [Trial.RUNNING, Trial.TERMINATED, Trial.PENDING]:
        for i in range(10):
            t = Trial("__fake")
            t.trial_id = str(i)
            t.set_status(status)
            t.last_result = {"episode_reward_mean": 100 + i}
            trials.append(t)
    table_data = _get_trial_table_data(trials, ["episode_reward_mean"])
    header = table_data.header
    assert header == ["Trial name", "status", "reward"]
    table_data = table_data.data
    assert len(table_data) == 3  # only the running category
    for i in range(3):
        assert len(table_data[i].trial_infos) == 5
    assert table_data[0].more_info == "... and 5 more RUNNING ..."
    assert table_data[1].more_info == "... and 5 more TERMINATED ..."
    assert table_data[2].more_info == "... and 5 more PENDING ..."


def test_result_table_no_divison():
    data = _get_dict_as_table_data(
        {
            "b": 6,
            "a": 8,
            "x": 19.123123123,
            "c": 5,
            "ignore": 9,
            "y": 20,
            "z": {"m": 4, "n": {"o": "p"}},
        },
        exclude={"ignore"},
    )

    assert data == [
        ["a", 8],
        ["b", 6],
        ["c", 5],
        ["x", "19.12312"],
        ["y", 20],
        ["z", None],
        ["/m", 4],
        ["/n", None],
        ["//o", "p"],
    ]


def test_result_table_divison():
    data = _get_dict_as_table_data(
        {
            "b": 6,
            "a": 8,
            "x": 19.123123123,
            "c": 5,
            "ignore": 9,
            "y": 20,
            "z": {"m": 4, "n": {"o": "p"}},
        },
        exclude={"ignore"},
        upper_keys={"x", "y", "z"},
    )

    assert data == [
        ["x", "19.12312"],
        ["y", 20],
        ["z", None],
        ["/m", 4],
        ["/n", None],
        ["//o", "p"],
        ["a", 8],
        ["b", 6],
        ["c", 5],
    ]


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
