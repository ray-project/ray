# Forked from https://github.com/ray-project/ray/blob/master/doc/source/ray-core/examples/automl_for_time_series.ipynb.
# The document https://docs.ray.io/en/latest/ray-core/examples/automl_for_time_series.html.
import ray
import time
import itertools
from collections import defaultdict
import asyncio
import logging
import sys

logging.basicConfig(stream=sys.stdout, format='%(asctime)s %(message)s', level=logging.DEBUG)
logger = logging.getLogger(__name__)

@ray.remote(runtime_env={
    "pip": ["statsforecast", "scikit-learn"]
})
def train_and_evaluate_fold(
    model,
    df,
    train_indices,
    test_indices,
    label_column,
    metrics,
    freq
):
    from statsforecast import StatsForecast
    try:
        # Create the StatsForecast object with train data & model.
        statsforecast = StatsForecast(
            df=df.iloc[train_indices], models=[model], freq=freq
        )
        # Make a forecast and calculate metrics on test data.
        # This will fit the model first automatically.
        forecast = statsforecast.forecast(len(test_indices))
        return {
            metric_name: metric(
                df.iloc[test_indices][label_column], forecast[model.__class__.__name__]
            )
            for metric_name, metric in metrics.items()
        }
    except Exception:
        # In case the model fit or eval fails, return None for all metrics.
        return {metric_name: None for metric_name, metric in metrics.items()}


def evaluate_models_with_cv(
    models,
    df,
    label_column,
    metrics,
    freq,
    cv,
):
    from sklearn.model_selection import TimeSeriesSplit
    import numpy as np
    # Obtain CV train-test indices for each fold.
    if isinstance(cv, int):
        cv = TimeSeriesSplit(cv)
    train_test_indices = list(cv.split(df))

    # Put df into Ray object store for better performance.
    df_ref = ray.put(df)

    # Add tasks to be executed for each fold.
    fold_refs = []
    for model in models:
        fold_refs.extend(
            [
                train_and_evaluate_fold.remote(
                    model,
                    df_ref,
                    train_indices,
                    test_indices,
                    label_column,
                    metrics,
                    freq=freq,
                )
                for train_indices, test_indices in train_test_indices
            ]
        )

    fold_results = ray.get(fold_refs)

    # Split fold results into a list of CV splits-sized chunks.
    # Ray guarantees that order is preserved.
    fold_results_per_model = [
        fold_results[i : i + len(train_test_indices)]
        for i in range(0, len(fold_results), len(train_test_indices))
    ]

    # Aggregate and average results from all folds per model.
    # We go from a list of dicts to a dict of lists and then
    # get a mean of those lists.
    mean_results_per_model = []
    for model_results in fold_results_per_model:
        aggregated_results = defaultdict(list)
        for fold_result in model_results:
            for metric, value in fold_result.items():
                aggregated_results[metric].append(value)
        mean_results = {
            metric: np.mean(values) for metric, values in aggregated_results.items()
        }
        mean_results_per_model.append(mean_results)

    # Join models and their metrics together.
    mean_results_per_model = {
        models[i]: mean_results_per_model[i] for i in range(len(mean_results_per_model))
    }
    return mean_results_per_model


def generate_configurations(search_space):
    # Convert dict search space into configurations - models instantiated with specific arguments.
    for model, model_search_space in search_space.items():
        kwargs, values = model_search_space.keys(), model_search_space.values()
        # Get a product - all combinations in the per-model grid.
        for configuration in itertools.product(*values):
            yield model(**dict(zip(kwargs, configuration)))


def evaluate_search_space_with_cv(
    search_space,
    df,
    label_column,
    metrics,
    eval_metric,
    mode = "min",
    freq = "D",
    cv = 5,
):
    assert eval_metric in metrics
    assert mode in ("min", "max")

    configurations = list(generate_configurations(search_space))
    logger.info(
        f"Evaluating {len(configurations)} configurations with {cv.get_n_splits()} splits each, "
        f"totalling {len(configurations)*cv.get_n_splits()} tasks..."
    )
    ret = evaluate_models_with_cv(
        configurations, df, label_column, metrics, freq=freq, cv=cv
    )

    # Sort the results by eval_metric
    ret = sorted(ret.items(), key=lambda x: x[1][eval_metric], reverse=(mode == "max"))
    logger.info("Evaluation complete!")
    return ret


def get_m5_partition(source: str, unique_id: str):
    from pyarrow import parquet as pq
    import pandas as pd
    ds1 = pq.read_table(
        # "s3://anonymous@m5-benchmarks/data/train/target.parquet",
        source,
        filters=[("item_id", "=", unique_id)],
    )
    Y_df = ds1.to_pandas()
    # StatsForecasts expects specific column names!
    Y_df = Y_df.rename(
        columns={"item_id": "unique_id", "timestamp": "ds", "demand": "y"}
    )
    Y_df["unique_id"] = Y_df["unique_id"].astype(str)
    Y_df["ds"] = pd.to_datetime(Y_df["ds"])
    Y_df = Y_df.dropna()
    constant = 10
    Y_df["y"] += constant
    return Y_df[Y_df.unique_id == unique_id]


@ray.remote(runtime_env={
    "pip": ["statsforecast", "scikit-learn"]
})
class Trainer:
    def __init__(self, data_source, data_partition, model_season_length, model):
        self._data_source = data_source
        self._data_partition = data_partition
        self._model_season_length = model_season_length
        self._model = model
    
    def train(self):
        df = get_m5_partition(self._data_source, self._data_partition)

        logger.info(f"df {df}")

        from statsforecast.models import ETS, AutoARIMA
        from sklearn.model_selection import TimeSeriesSplit
        from sklearn.metrics import mean_squared_error, mean_absolute_error

        tuning_results = evaluate_search_space_with_cv(
            {AutoARIMA: {}, ETS: {"season_length": self._model_season_length, "model": self._model}},
            df,
            "y",
            {"mse": mean_squared_error, "mae": mean_absolute_error},
            "mse",
            cv=TimeSeriesSplit(test_size=1),
        )

        logger.info(tuning_results[0])
        return tuning_results[0][0].__dict__


@ray.remote
class Proxy:

    class Context:
        def __init__(self, actor_handle, result_object, result = None):
            self.actor_handle = actor_handle
            self.result_object = result_object
            self.result = result

    def __init__(self):
        self._next_task_id = 0
        self._tasks = {}
        loop = asyncio.get_event_loop()
        loop.create_task(self._start_monitor())
    
    async def _start_monitor(self):
        while True:
            for i, context in self._tasks.items():
                if context.result:
                    continue
                try:
                    finished, pending = await asyncio.wait([context.result_object], timeout=0.1)
                    if finished:
                        context.result = ray.get(context.result_object)
                        logger.info(f"The task {i} finished, result: {context.result}.")
                        ray.kill(context.actor_handle)
                except Exception:
                    logger.exception("Error in monitor.")
            await asyncio.sleep(5)

    async def do_auto_ml(self, data_source, data_partition, model_season_length, model):
        actor_handle = Trainer.remote(data_source, data_partition, model_season_length, model)
        result_object = actor_handle.train.remote()
        task_id = self._next_task_id
        self._next_task_id += 1
        self._tasks[task_id] = Proxy.Context(actor_handle, result_object)
        return task_id
    
    async def get_result(self, task_id):
        if task_id not in self._tasks:
            raise ValueError(f"unknown task id {task_id}")
        return self._tasks[task_id].result


ray.init(ignore_reinit_error=True)

proxy = Proxy.remote()
task_id = ray.get(proxy.do_auto_ml.remote(
    "s3://anonymous@m5-benchmarks/data/train/target.parquet", 
    "FOODS_1_001_CA_1", 
    [6, 7],
    ["ZNA", "ZZZ"],
))

while True:
    result = ray.get(proxy.get_result.remote(task_id))
    if result:
        logger.info(f"The task has already finished, the result {result}.")
        break
    else:
        logger.info(f"The task is not finished, wait...")
    time.sleep(5)

logger.info("Finished!")
