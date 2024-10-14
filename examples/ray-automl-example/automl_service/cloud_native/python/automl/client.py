import grpc
from automl.generated import (
    automl_service_pb2,
    automl_service_pb2_grpc,
)

class AutoMLClient:
    def __init__(self, proxy_address):
        self._proxy_address = proxy_address
        channel = grpc.insecure_channel(proxy_address)
        self._stab = automl_service_pb2_grpc.AutoMLServiceStub(channel)
    
    def do_auto_ml(self, data_source, data_partition, model_season_lengths, models):
        response = self._stab.DoAutoML(
            automl_service_pb2.DoAutoMLRequest(
                data_source=data_source,
                data_partition=data_partition,
                model_season_lengths=model_season_lengths,
                models=models,
            )
        )
        if not response.success:
            raise RuntimeError(f"Failed to request automl service: {response.message}")
        return response.task_id
    
    def get_result(self, task_id):
        response = self._stab.GetResult(
            automl_service_pb2.GetResultRequest(
                task_id=task_id,
            )
        )
        if not response.success:
            raise RuntimeError(f"Failed to get result from automl service: {response.message}")
        return response.result
