import time

from ray import serve


@serve.deployment
class Preprocessor:
    def __call__(self, input_data: str) -> str:
        time.sleep(0.01)
        return f"preprocessed_{input_data}"


@serve.deployment
class Model:
    def __call__(self, preprocessed_data: str) -> str:
        time.sleep(0.02)
        return f"result_{preprocessed_data}"


@serve.deployment
class Driver:
    def __init__(self, preprocessor, model):
        self._preprocessor = preprocessor
        self._model = model

    async def __call__(self, input_data: str) -> str:
        preprocessed = await self._preprocessor.remote(input_data)
        result = await self._model.remote(preprocessed)
        return result


app = Driver.bind(Preprocessor.bind(), Model.bind())
