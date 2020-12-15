# Notes on MLflow:
# Load mlflow model within backend
# Use pandas dataframe json for input web request
# For update deployment, only load new backend if different model uri, otherwise just call update_backend_config with config arg

import ray
from ray import serve
class MLFlowBackend:
	def __init__(self, model_uri):
		self.model = mlflow.pyfunc_from_model(_download_artifact_from_uri(model_uri))
	async def __call__(self, request):
		df = pandas.load_from_json(await request.json())
		return self.model(df)
client.create_backend("mlflow", MLFlowBackend, "http://hello_archit.com")