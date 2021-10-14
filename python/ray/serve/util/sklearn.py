from typing import Callable, List, Union

import pandas as pd

from starlette.requests import Request

from ray import serve


class SKLearnModel:
    def __new__(cls,
                *,
                max_batch_size: int = 10,
                batch_wait_timeout_s: float = 0.0):
        cls._do_batch = serve.batch(
            max_batch_size=max_batch_size,
            batch_wait_timeout_s=batch_wait_timeout_s)(cls._do_batch)

        return super().__new__(cls)

    def __init__(self,
                 model: Callable,
                 max_batch_size: int = 10,
                 batch_wait_timeout_s: float = 0.0):
        self._model = model

    async def _do_batch(self, dfs: List[Request]):
        df = pd.concat(dfs, ignore_index=True)
        return self._model.predict(df).tolist()

    async def __call__(self, request: Union[pd.DataFrame, Request]):
        df = None
        if isinstance(request, pd.DataFrame):
            # TODO: validate DataFrame shape.
            df = request
        elif isinstance(request, Request):
            df = pd.DataFrame(await request.json())
        else:
            raise TypeError(
                "SKLearn model supports JSON HTTP payload or pandas DataFrame."
            )

        # TODO: handle multiple rows in the input.
        return [await self._do_batch(df)]
