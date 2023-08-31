from contextlib import contextmanager
import tempfile
import os
from pathlib import Path
import pickle
import pandas as pd
import numpy as np
from typing import List

import pytest

from ray import train, tune
from ray.air._internal.uri_utils import URI
from ray.air.constants import EXPR_PROGRESS_FILE, EXPR_RESULT_FILE
from ray.train._internal.storage import _delete_fs_path
from ray.tune.analysis.experiment_analysis import NewExperimentAnalysis
from ray.tune.experiment import Trial
from ray.tune.utils import flatten_dict

from ray.train.tests.util import (
    create_dict_checkpoint,
    load_dict_checkpoint,
    mock_s3_bucket_uri,
)


NUM_TRIALS = 3
NON_NAN_VALUE = 42
PEAK_VALUE = 100


def train_fn(config):
    def report(metrics, should_checkpoint=True):
        if should_checkpoint:
            with create_dict_checkpoint(metrics) as checkpoint:
                train.report(metrics, checkpoint=checkpoint)
        else:
            train.report(metrics)

    id = config["id"]

    report({"ascending": 1 * id, "peak": 0, "maybe_nan": np.nan, "iter": 1})
    report({"ascending": 2 * id, "peak": 0, "maybe_nan": np.nan, "iter": 2})
    report({"ascending": 3 * id, "peak": 0, "maybe_nan": np.nan, "iter": 3})
    report(
        {
            "ascending": 4 * id,
            "peak": 0,
            "maybe_nan": NON_NAN_VALUE,
            "iter": 4,
        }
    )
    report({"ascending": 5 * id, "peak": PEAK_VALUE, "maybe_nan": np.nan, "iter": 5})

    report(
        {"ascending": 6 * id, "peak": -PEAK_VALUE, "maybe_nan": np.nan, "iter": 6},
        should_checkpoint=False,
    )
    report(
        {"ascending": 7 * id, "peak": 0, "maybe_nan": np.nan, "iter": 7},
        should_checkpoint=False,
    )


def _get_trial_with_id(trials: List[Trial], id: int) -> Trial:
    return [trial for trial in trials if trial.config["id"] == id][0]


@contextmanager
def dummy_context_manager():
    yield "dummy value"


@pytest.fixture(scope="module", params=["dir", "memory", "cloud"])
def experiment_analysis(request):
    load_from = request.param
    tmp_path = Path(tempfile.mkdtemp())

    os.environ["RAY_AIR_LOCAL_CACHE_DIR"] = str(tmp_path / "ray_results")

    context_manager = (
        mock_s3_bucket_uri if load_from == "cloud" else dummy_context_manager
    )

    with context_manager() as cloud_storage_path:
        storage_path = (
            str(cloud_storage_path)
            if load_from == "cloud"
            else str(tmp_path / "fake_nfs")
        )
        ea = tune.run(
            train_fn,
            config={"id": tune.grid_search(list(range(1, NUM_TRIALS + 1)))},
            metric="ascending",
            mode="max",
            storage_path=storage_path,
            name="test_experiment_analysis",
        )

        if load_from in ["dir", "cloud"]:
            # Test init without passing in in-memory trials.
            # Load them from an experiment directory instead.
            yield NewExperimentAnalysis(
                str(URI(storage_path) / "test_experiment_analysis"),
                default_metric="ascending",
                default_mode="max",
            )
        elif load_from == "memory":
            yield ea
        else:
            raise NotImplementedError(f"Invalid param: {load_from}")


@pytest.mark.parametrize("filetype", ["json", "csv"])
def test_fetch_trial_dataframes(experiment_analysis, filetype):
    if filetype == "csv":
        # Delete all json files so that we can test fallback to csv loading
        for trial in experiment_analysis.trials:
            _delete_fs_path(
                fs=trial.storage.storage_filesystem,
                fs_path=os.path.join(trial.storage.trial_fs_path, EXPR_RESULT_FILE),
            )
    else:
        assert filetype == "json"

    dfs = experiment_analysis._fetch_trial_dataframes()
    assert len(dfs) == NUM_TRIALS
    assert all(isinstance(df, pd.DataFrame) for df in dfs.values())
    assert {trial.trial_id for trial in experiment_analysis.trials} == set(dfs)

    for trial_id, df in dfs.items():
        trial_config = experiment_analysis.get_all_configs()[trial_id]
        assert np.all(
            df["ascending"].to_numpy() == np.arange(1, 8) * trial_config["id"]
        )


def test_fetch_trial_dataframes_with_errors(
    experiment_analysis, tmp_path, propagate_logs, caplog
):
    # Add "corrupted" json files)
    for trial in experiment_analysis.trials:
        fs = trial.storage.storage_filesystem
        with fs.open_output_stream(
            os.path.join(trial.storage.trial_fs_path, EXPR_RESULT_FILE)
        ) as f:
            f.write(b"malformed")

    experiment_analysis._fetch_trial_dataframes()
    assert "Failed to fetch metrics" in caplog.text
    caplog.clear()

    # Delete ALL metrics files to check that a warning gets logged.
    for trial in experiment_analysis.trials:
        fs = trial.storage.storage_filesystem

        # Delete ALL metrics files to check that a warning gets logged.
        _delete_fs_path(
            fs=trial.storage.storage_filesystem,
            fs_path=os.path.join(trial.storage.trial_fs_path, EXPR_RESULT_FILE),
        )
        _delete_fs_path(
            fs=trial.storage.storage_filesystem,
            fs_path=os.path.join(trial.storage.trial_fs_path, EXPR_PROGRESS_FILE),
        )

    experiment_analysis._fetch_trial_dataframes()
    assert "Could not fetch metrics for" in caplog.text
    assert "FileNotFoundError" in caplog.text
    caplog.clear()


def test_get_all_configs(experiment_analysis):
    configs = experiment_analysis.get_all_configs()
    assert len(configs) == NUM_TRIALS
    assert all(isinstance(config, dict) for config in configs.values())
    assert {trial.trial_id for trial in experiment_analysis.trials} == set(configs)
    for trial_id, config in configs.items():
        trial = [
            trial for trial in experiment_analysis.trials if trial.trial_id == trial_id
        ][0]
        assert trial.config == config


def test_dataframe(experiment_analysis):
    with pytest.raises(ValueError):
        # Invalid mode
        df = experiment_analysis.dataframe(mode="bad")

    with pytest.raises(ValueError):
        # Should raise because we didn't pass a metric
        df = experiment_analysis.dataframe(mode="max")

    # If we specify `max`, we expect the largets ever observed result
    df = experiment_analysis.dataframe(metric="peak", mode="max")
    assert df.iloc[0]["peak"] == PEAK_VALUE

    # If we specify `min`, we expect the lowest ever observed result
    df = experiment_analysis.dataframe(metric="peak", mode="min")
    assert df.iloc[0]["peak"] == -PEAK_VALUE

    # If we don't pass a mode, we just fetch the last result
    df = experiment_analysis.dataframe(metric="peak")
    assert df.iloc[0]["peak"] == 0
    assert df.iloc[0]["iter"] == 7


def test_default_properties(experiment_analysis):
    # The last trial has the highest score (according to the default metric/mode).
    best_trial = _get_trial_with_id(experiment_analysis.trials, NUM_TRIALS)
    assert experiment_analysis.best_trial == best_trial
    assert experiment_analysis.best_config == best_trial.config
    # The last (most recent) checkpoint has the highest score.
    assert experiment_analysis.best_checkpoint == best_trial.checkpoint
    # NaN != NaN, so fill them in for this equality check.
    assert experiment_analysis.best_dataframe.fillna(-1).equals(
        experiment_analysis.trial_dataframes[best_trial.trial_id].fillna(-1)
    )
    assert experiment_analysis.best_result == best_trial.last_result

    result_df_dict = experiment_analysis.best_result_df.iloc[0].to_dict()
    # Converting -> pandas -> dict flattens the dict.
    best_trial_dict = flatten_dict(best_trial.last_result, delimiter="/")
    assert result_df_dict["ascending"] == best_trial_dict["ascending"]

    assert len(experiment_analysis.results) == NUM_TRIALS
    assert len(experiment_analysis.results_df) == NUM_TRIALS


def test_get_best_config(experiment_analysis):
    assert experiment_analysis.get_best_config()["id"] == NUM_TRIALS
    assert (
        experiment_analysis.get_best_config(metric="ascending", mode="min")["id"] == 1
    )

    assert not experiment_analysis.get_best_config(metric="maybe_nan", scope="last")


def test_get_best_trial(experiment_analysis):
    assert (
        experiment_analysis.get_best_trial().config
        == experiment_analysis.get_best_config()
    )

    assert not experiment_analysis.get_best_trial(metric="maybe_nan")
    assert experiment_analysis.get_best_trial(
        metric="maybe_nan", filter_nan_and_inf=False
    )


def test_get_best_checkpoint(experiment_analysis):
    best_trial = experiment_analysis.get_best_trial()
    best_checkpoint = load_dict_checkpoint(
        experiment_analysis.get_best_checkpoint(best_trial)
    )
    # NOTE: There are 7 reports, but only the first 5 include a checkpoint.
    assert best_checkpoint["ascending"] == 5 * NUM_TRIALS

    best_checkpoint = load_dict_checkpoint(
        experiment_analysis.get_best_checkpoint(
            best_trial, metric="ascending", mode="min"
        )
    )
    assert best_checkpoint["ascending"] == 1 * NUM_TRIALS

    # Filter checkpoints w/ NaN metrics
    best_checkpoint = load_dict_checkpoint(
        experiment_analysis.get_best_checkpoint(best_trial, metric="maybe_nan")
    )
    assert best_checkpoint["maybe_nan"] == NON_NAN_VALUE


def test_get_last_checkpoint(experiment_analysis):
    # Defaults to getting the last checkpoint of the best trial.
    last_checkpoint = load_dict_checkpoint(experiment_analysis.get_last_checkpoint())
    assert last_checkpoint["iter"] == 5  # See note

    last_checkpoint = load_dict_checkpoint(
        experiment_analysis.get_last_checkpoint(
            trial=_get_trial_with_id(experiment_analysis.trials, 1)
        )
    )
    assert last_checkpoint["ascending"] == 5 * 1  # See note


def test_pickle(experiment_analysis, tmp_path):
    pickle_path = os.path.join(tmp_path, "analysis.pkl")
    with open(pickle_path, "wb") as f:
        pickle.dump(experiment_analysis, f)

    assert experiment_analysis.get_best_trial(metric="ascending", mode="max")

    with open(pickle_path, "rb") as f:
        loaded_analysis = pickle.load(f)

    assert loaded_analysis.get_best_trial(metric="ascending", mode="max")


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
