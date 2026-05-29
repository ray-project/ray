# serve_forecast_multiplex.py
import asyncio
import numpy as np
import pickle
from ray import serve
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


@serve.deployment
class ForecastingService:
    def __init__(self):
        # In production, this is your cloud storage path or model registry
        self.model_storage_path = "/customer-models"
    
    @serve.multiplexed(max_num_models_per_replica=4)
    async def get_model(self, customer_id: str):
        """Load a customer's forecasting model.
        
        In production, this function downloads from cloud storage or loads from a database.
        For this example, the code mocks the I/O with asyncio.sleep().
        """
        # Simulate downloading model from remote storage
        await asyncio.sleep(0.1)  # Mock network I/O delay
        
        # In production:
        # model_bytes = await download_from_storage(f"{self.model_storage_path}/{customer_id}/model.pkl")
        # return pickle.loads(model_bytes)
        
        # For this example, create a mock model
        return ForecastModel(customer_id)
    
    async def __call__(self, request: Request):
        """Generate forecast for a customer."""
        # Get the serve_multiplexed_model_id from the request header
        customer_id = serve.get_multiplexed_model_id()
        
        # Load the model (cached if already loaded)
        model = await self.get_model(customer_id)
        
        # Get input data
        data = await request.json()
        sequence_data = data.get("sequence_data", [])
        
        # Generate forecast
        forecast = model.predict(sequence_data)
        
        return {"customer_id": customer_id, "forecast": forecast}


app = ForecastingService.bind()
