import argparse
import asyncio
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

import logging
import sys

logging.basicConfig(stream=sys.stdout, format='%(asctime)s %(message)s', level=logging.DEBUG)
logger = logging.getLogger(__name__)

class Proxy(automl_service_pb2_grpc.AutoMLServiceServicer,
            automl_service_pb2_grpc.TrainerRegisterServiceServicer):
    class Context:
        def __init__(self, data_source, data_partition, model_season_lengths, models, trainer_id, result = None):
            self.data_source = data_source
            self.data_partition = data_partition
            self.model_season_lengths = model_season_lengths
            self.models = models
            self.trainer_id = trainer_id
            self.result = result

    def __init__(
        self,
        grpc_port: int,
        host_name: str,
        operator_address: str,
    ):

        self.server = aiogrpc.server(options=(("grpc.so_reuseport", 0),))
        grpc_ip = "0.0.0.0"
        self.grpc_port = self.server.add_insecure_port(f"{grpc_ip}:{grpc_port}")
        logger.info("Proxy grpc address: %s:%s", grpc_ip, self.grpc_port)
        self._host_name = host_name
        self._operator_client = OperatorClient(operator_address)
        self._next_task_id = 0
        self._tasks = {}
    
    async def DoAutoML(self, request, context):
        logger.info(f"Do auto ml request: {request.data_source} {request.data_partition} {request.model_season_lengths} {request.models}")
        task_id = self._next_task_id
        self._next_task_id += 1
        trainer_id = self._operator_client.start_trainer("2345", f"{self._host_name}:{self.grpc_port}", task_id, "")
        self._tasks[task_id] = Proxy.Context(request.data_source, request.data_partition, request.model_season_lengths, request.models, trainer_id)
        return automl_service_pb2.DoAutoMLReply(
            success=True,
            task_id=task_id,
            message="test",
        )
    
    async def GetResult(self, request, context):
        if request.task_id not in self._tasks:
            raise ValueError(f"unknown task id {request.task_id}")     
        return automl_service_pb2.GetResultReply(
            success=True,
            result=self._tasks[request.task_id].result,
        )

    async def Register(self, request, context):
        if request.task_id not in self._tasks:
            return automl_service_pb2.RegisterReply(
                success=False,
                message=f"Task id {request.task_id} not found.",
            )
        context = self._tasks[request.task_id]
        return automl_service_pb2.RegisterReply(
            success=True,
            data_source=context.data_source,
            data_partition=context.data_partition,
            model_season_lengths=context.model_season_lengths,
            models=context.models,
        )

    async def ReportResult(self, request, context):
        logger.info(f"Receive report result with task id {request.task_id}.")
        if request.task_id not in self._tasks:
            return automl_service_pb2.ReportResultReply(
                success=False,
                message=f"Task id {request.task_id} not found.",
            )
        self._tasks[request.task_id].result = request.result
        return automl_service_pb2.ReportResultReply(
            success=True,
        )

    async def run(self):

        # Start a grpc asyncio server.
        await self.server.start()

        automl_service_pb2_grpc.add_AutoMLServiceServicer_to_server(
            self, self.server
        )

        automl_service_pb2_grpc.add_TrainerRegisterServiceServicer_to_server(
            self, self.server
        )

        await self.server.wait_for_termination()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="AutoML proxy.")
    parser.add_argument(
        "--port", required=True, type=int, help="The grpc port."
    )
    parser.add_argument(
        "--host-name", required=True, type=str, help="The current host name."
    )
    parser.add_argument(
        "--operator-address", required=True, type=str, help="The automl operator address."
    )

    args = parser.parse_args()

    proxy = Proxy(args.port, args.host_name, args.operator_address)

    loop = get_or_create_event_loop()

    loop.run_until_complete(proxy.run())
