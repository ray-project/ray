import argparse
import asyncio
import pickle
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

class Worker:

    def __init__(
        self,
        args,
    ):
        self._worker_id = args.worker_id
        self._trainer_address = args.trainer_address
        self._result = None
    
    async def RegisterToTrainer(self):
        async with aiogrpc.insecure_channel(self._trainer_address) as channel:
            stub = automl_service_pb2_grpc.WorkerRegisterServiceStub(channel)
            response = await stub.WorkerRegister(automl_service_pb2.WorkerRegisterRequest(
                worker_id=self._worker_id,
            ))
            if not response.success:
                raise RuntimeError(f"Failed to register to trainer {self._trainer_address}")
            self._worker_task = pickle.loads(response.worker_task)

    async def ReportResultToTrainer(self):
        async with aiogrpc.insecure_channel(self._trainer_address) as channel:
            stub = automl_service_pb2_grpc.WorkerRegisterServiceStub(channel)
            response = await stub.WorkerReportResult(automl_service_pb2.WorkerReportResultRequest(
                worker_id=self._worker_id,
                result= self._result,
            ))
            if not response.success:
                raise RuntimeError(f"Failed to report result to proxy {self._proxy_address}")
    

    def train_and_evaluate_fold(
        self,
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



    async def run(self):

        # register
        await self.RegisterToTrainer()

        result = self.train_and_evaluate_fold(
            self._worker_task.model,
            self._worker_task.df,
            self._worker_task.train_indices,
            self._worker_task.test_indices,
            self._worker_task.label_column,
            self._worker_task.metrics,
            self._worker_task.freq,
        )
        self._result = result

        await self.ReportResultToTrainer()

        logger.info("Training worker task finished! Exit...")
        sys.exit(0)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="AutoML worker.")
    parser.add_argument(
        "--trainer-address", required=True, type=str, help="The grpc address of automl trainer."
    )
    parser.add_argument(
        "--worker-id", required=True, type=str, help="The id of current worker."
    )

    args = parser.parse_args()

    worker = Worker(args)

    loop = get_or_create_event_loop()

    loop.run_until_complete(worker.run())
