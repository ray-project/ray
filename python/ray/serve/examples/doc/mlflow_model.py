import ray
from ray import serve

import mlflow.pyfunc


class MLFlowBackend:
    def __init__(self, model_uri):
        self.model = mlflow.pyfunc.load_model(model_uri=model_uri)

    async def __call__(self, request):
        return self.model.predict(request.data)


ray.init()
client = serve.start()

model_uri = "/Users/ray_user/my_mlflow_model"
client.create_backend("mlflow_backend", MLFlowBackend, model_uri)
