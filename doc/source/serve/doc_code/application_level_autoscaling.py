# __serve_example_begin__
import time
from ray import serve


@serve.deployment
class Preprocessor:
    def __call__(self, input_data: str) -> str:
        # Simulate preprocessing work
        time.sleep(0.05)
        return f"preprocessed_{input_data}"


@serve.deployment
class Model:
    def __call__(self, preprocessed_data: str) -> str:
        # Simulate model inference (takes longer than preprocessing)
        time.sleep(0.1)
        return f"result_{preprocessed_data}"


@serve.deployment
class Driver:
    def __init__(self, preprocessor, model):
        self._preprocessor = preprocessor
        self._model = model

    async def __call__(self, input_data: str) -> str:
        # Coordinate preprocessing and model inference
        preprocessed = await self._preprocessor.remote(input_data)
        result = await self._model.remote(preprocessed)
        return result


app = Driver.bind(Preprocessor.bind(), Model.bind())
# __serve_example_end__
