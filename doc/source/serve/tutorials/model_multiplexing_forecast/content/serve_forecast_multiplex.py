# serve_forecast_multiplex.py
import asyncio
import numpy as np
import pickle
from ray import serve
from ray.serve.handle import DeploymentHandle
from starlette.requests import Request


# Simple forecasting model
class ForecastModel:
    """A customer-specific forecasting model."""

    def __init__(self, customer_id: str):
        self.customer_id = customer_id
        # Each customer has different model parameters
        np.random.seed(hash(customer_id) % 1000)
        self.trend = np.random.uniform(-1, 3)
        self.base_level = np.random.uniform(90, 110)

    def predict(self, sequence_data: list) -> list:
        """Generate a 7-day forecast."""
        last_value = sequence_data[-1] if sequence_data else self.base_level
        forecast = []
        for i in range(7):
            # Simple forecast: last value + trend
            value = last_value + self.trend * (i + 1)
            forecast.append(round(value, 2))
        return forecast


# Multiplexing belongs on the downstream deployment, not the ingress.
@serve.deployment
class ForecastingModel:
    """Downstream deployment with multiplexed model loading."""

    def __init__(self):
        self.model_storage_path = "/customer-models"

    @serve.multiplexed(max_num_models_per_replica=4)
    async def get_model(self, customer_id: str):
        """Load a customer's forecasting model."""
        await asyncio.sleep(0.1)  # Mock network I/O delay
        return ForecastModel(customer_id)

    async def __call__(self, sequence_data: list):
        customer_id = serve.get_multiplexed_model_id()
        model = await self.get_model(customer_id)
        forecast = model.predict(sequence_data)
        return {"customer_id": customer_id, "forecast": forecast}


@serve.deployment
class ForecastingIngress:
    """Ingress: extracts customer ID from request and forwards to multiplexed downstream."""

    def __init__(self, model_deployment: DeploymentHandle):
        self._model = model_deployment

    async def __call__(self, request: Request):
        customer_id = request.headers.get("serve_multiplexed_model_id", "default")
        data = await request.json()
        sequence_data = data.get("sequence_data", [])
        return await self._model.options(
            multiplexed_model_id=customer_id
        ).remote(sequence_data)


app = ForecastingIngress.bind(ForecastingModel.bind())
