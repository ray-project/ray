from typing import Optional

import pandas as pd

import ray
from ray.data import ActorPoolStrategy
from ray.data.preprocessor import Preprocessor
from ray.train.predictor import Predictor


class DummyPreprocessor(Preprocessor):
    _is_fittable = False

    def __init__(self, multiplier=2):
        self.multiplier = multiplier

    def _transform_pandas(self, df):
        return df * self.multiplier


class DummyPredictor(Predictor):
    def __init__(
        self,
        factor: float = 1.0,
        preprocessor: Optional[Preprocessor] = None,
        use_gpu: bool = False,
    ):
        self.factor = factor
        self.use_gpu = use_gpu
        super().__init__(preprocessor)

    def _predict_pandas(self, data: pd.DataFrame, **kwargs) -> pd.DataFrame:
        return data * self.factor


predictor = DummyPredictor(factor=2)
ds = ray.data.range_table(4)
ds = ds.map_batches(predictor, compute=ActorPoolStrategy(4, 4), fn_kwargs={"feature_columns": ["value"], "keep_columns": ["value"]})
ds.cache()

# assert ds.to_pandas().to_numpy().squeeze().tolist() == [
#     0.0,
#     2.0,
#     4.0,
#     6.0,
# ]


# predictor = DummyPredictor(factor=2, preprocessor=DummyPreprocessor())
# ds = ray.data.range_table(4)
# ds = ds.map_batches(predictor, compute=ActorPoolStrategy(4, 4))

# assert ds.to_pandas().to_numpy().squeeze().tolist() == [
#     0.0,
#     4.0,
#     8.0,
#     12.0,
# ]
