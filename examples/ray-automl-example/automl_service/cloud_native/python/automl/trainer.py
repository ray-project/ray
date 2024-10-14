import argparse
import asyncio
import itertools
import pickle
from collections import defaultdict
import numpy as np

try:
    from grpc import aio as aiogrpc
except ImportError:
    from grpc.experimental import aio as aiogrpc

from automl.generated import (
    automl_service_pb2,
    automl_service_pb2_grpc,
)

from automl.operator import OperatorClient
from automl.utils import get_or_create_event_loop
from automl.common import WorkerTask

import logging
import sys
import collections

logging.basicConfig(stream=sys.stdout, format='%(asctime)s %(message)s', level=logging.DEBUG)
logger = logging.getLogger(__name__)

def evaluate_models_with_cv(
    models,
    df,
    label_column,
    metrics,
    freq,
    cv,
    mode,
):
    from sklearn.model_selection import TimeSeriesSplit
    import numpy as np
    # Obtain CV train-test indices for each fold.
    if isinstance(cv, int):
        cv = TimeSeriesSplit(cv)
    train_test_indices = list(cv.split(df))

    # Add tasks to be executed for each fold.
    worker_tasks = []
    for model in models:
        worker_tasks.extend(
            [
                WorkerTask(model,df,train_indices,test_indices,label_column,metrics,freq)
                for train_indices, test_indices in train_test_indices
            ]
        )
    
    return models, mode, worker_tasks, len(train_test_indices)


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
    return evaluate_models_with_cv(
        configurations, df, label_column, metrics, freq=freq, cv=cv, mode=mode,
    )


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


class Trainer(automl_service_pb2_grpc.WorkerRegisterService):
    class TaskSpec:
        def __init__(self, data_source, data_partition, model_season_lengths, models):
            self.data_source = data_source
            self.data_partition = data_partition
            self.model_season_lengths = model_season_lengths
            self.models = models

    class WorkerContext:
        def __init__(self, worker_task: WorkerTask, result = {}):
            self.worker_task = worker_task
            self.metrics_result = result


    def __init__(
        self,
        args,
    ):

        self.server = aiogrpc.server(options=(("grpc.so_reuseport", 0),))
        grpc_ip = "0.0.0.0"
        self._grpc_port = self.server.add_insecure_port(f"{grpc_ip}:{args.grpc_port}")
        logger.info("Proxy grpc address: %s:%s", grpc_ip, self._grpc_port)
        self._host_name = args.host_name
        self._operator_client = OperatorClient(args.operator_address)
        self._proxy_address = args.proxy_address
        self._trainer_id = args.trainer_id
        self._task_id = args.task_id
        self._task_spec = None
        self._result = None
        self._worker_group_id = None
        self._workers = collections.OrderedDict()
        self._finished_worker_count = 0
        self._models = None
        self._mode = None
        self._train_test_indices_number = None
        self._eval_metric = None

    async def WorkerRegister(self, request, context):
        if request.worker_id not in self._workers:
            return automl_service_pb2.WorkerRegisterReply(
                success=False,
                message=f"Task id {request.worker_id} not found.",
            )
        logger.info(f"Worker {request.worker_id} registered.")
        context = self._workers[request.worker_id]
        return automl_service_pb2.WorkerRegisterReply(
            success=True,
            worker_task=pickle.dumps(context.worker_task),
            # model=,
            # df=,
            # train_indices=,
            # test_indices=,
            # label_column=,
            # metrics=,
            # freq=,
        )

    async def WorkerReportResult(self, request, context):
        if request.worker_id not in self._workers:
            return automl_service_pb2.WorkerReportResultReply(
                success=False,
                message=f"Task id {request.worker_id} not found.",
            )
        logger.info(f"Worker {request.worker_id} result reported.")
        self._workers[request.worker_id].metrics_result = request.result
        self._finished_worker_count += 1
        if self._finished_worker_count == len(self._workers):
            asyncio.create_task(self.train_finish())
        return automl_service_pb2.WorkerReportResultReply(
            success=True,
        )
    
    async def RegisterToProxy(self):
        async with aiogrpc.insecure_channel(self._proxy_address) as channel:
            stub = automl_service_pb2_grpc.TrainerRegisterServiceStub(channel)
            response = await stub.Register(automl_service_pb2.RegisterRequest(
                id=self._trainer_id,
                task_id=self._task_id,
            ))
            if not response.success:
                raise RuntimeError(f"Failed to register to proxy {self._proxy_address}")
            self._task_spec = Trainer.TaskSpec(
                response.data_source,
                response.data_partition,
                response.model_season_lengths,
                response.models,
            )

    async def ReportResultToProxy(self):
        async with aiogrpc.insecure_channel(self._proxy_address) as channel:
            stub = automl_service_pb2_grpc.TrainerRegisterServiceStub(channel)
            response = await stub.ReportResult(automl_service_pb2.ReportResultRequest(
                id=self._trainer_id,
                task_id=self._task_id,
                result=str(self._result),
            ))
            if not response.success:
                raise RuntimeError(f"Failed to report result to proxy {self._proxy_address}")


    def train_start(self):
        logger.info("Start loading data frame...")
        df = get_m5_partition(self._task_spec.data_source, self._task_spec.data_partition)

        logger.info(f"df {df}")

        from statsforecast.models import ETS, AutoARIMA
        from sklearn.model_selection import TimeSeriesSplit
        from sklearn.metrics import mean_squared_error, mean_absolute_error

        self._eval_metric = "mse"
        return evaluate_search_space_with_cv(
            {AutoARIMA: {}, ETS: {"season_length": self._task_spec.model_season_lengths, "model": self._task_spec.models}},
            df,
            "y",
            {"mse": mean_squared_error, "mae": mean_absolute_error},
            self._eval_metric,
            cv=TimeSeriesSplit(n_splits=2),
            # cv=TimeSeriesSplit(test_size=1),
        )
    
    async def train_finish(self):
        logger.info("Train finished, start aggragate the results...")
        # await self.ReportResultToProxy()

        fold_results = [worker_context.metrics_result for _, worker_context in self._workers.items()]

        # Split fold results into a list of CV splits-sized chunks.
        # Ray guarantees that order is preserved.
        fold_results_per_model = [
            fold_results[i : i + self._train_test_indices_number]
            for i in range(0, len(fold_results), self._train_test_indices_number)
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
        logger.info(f"self._task_spec.models size {len(self._task_spec.models)} mean_results_per_model size {len(mean_results_per_model)} ")
        mean_results_per_model = {
            self._models[i]: mean_results_per_model[i] for i in range(len(mean_results_per_model))
        }

        logger.info(f"mean_results_per_model.items() {mean_results_per_model.items()}")
        # Sort the results by eval_metric
        tuning_results = sorted(mean_results_per_model.items(), key=lambda x: x[1][self._eval_metric], reverse=(self._mode == "max"))
        logger.info("Evaluation complete!")
        self._result = tuning_results[0][0].__dict__
        logger.info(tuning_results[0])
        await self.ReportResultToProxy()
        logger.info("All training tasks finished! Exit...")
        sys.exit(0)


    async def run(self):

        # register
        await self.RegisterToProxy()

        models, mode, worker_tasks, train_test_indices_number = self.train_start()
        self._models = models
        self._mode = mode
        self._train_test_indices_number = train_test_indices_number

        # start worker group
        group_id, worker_ids = self._operator_client.start_worker_group(
            f"{self._host_name}:{self._grpc_port}", len(worker_tasks), None)
        logger.info(f"Starting worker group: the group id {group_id}, the worker ids {worker_ids}.")
        self._worker_group_id = group_id
        if len(worker_ids) != len(worker_tasks):
            raise RuntimeError(f"invalid worker_ids {worker_ids} from operator client. We need {len(worker_tasks)} ids.")
        for i, worker_id in enumerate(worker_ids):
            self._workers[worker_id] = Trainer.WorkerContext(worker_task=worker_tasks[i])

        # Start a grpc asyncio server.
        await self.server.start()

        automl_service_pb2_grpc.add_WorkerRegisterServiceServicer_to_server(
            self, self.server
        )

        await self.server.wait_for_termination()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="AutoML trainer.")
    parser.add_argument(
        "--grpc-port", required=True, type=int, help="The grpc port."
    )
    parser.add_argument(
        "--host-name", required=True, type=str, help="The current host name."
    )
    parser.add_argument(
        "--proxy-address", required=True, type=str, help="The grpc address of automl proxy."
    )
    parser.add_argument(
        "--trainer-id", required=True, type=str, help="The current trainer id."
    )
    parser.add_argument(
        "--task-id", required=True, type=int, help="The automl task id."
    )
    parser.add_argument(
        "--operator-address", required=True, type=str, help="The automl operator address."
    )

    args = parser.parse_args()

    trainer = Trainer(args)

    loop = get_or_create_event_loop()

    loop.run_until_complete(trainer.run())
