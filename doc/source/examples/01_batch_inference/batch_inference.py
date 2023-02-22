import pandas as pd

import ray
from ray.air import Checkpoint
from ray.data.preprocessors import BatchMapper
from ray.air.util.data_batch_conversion import BatchFormat
from ray.train.predictor import Predictor
from ray.train.batch_predictor import BatchPredictor


def load_trained_model():
    def model(batch: pd.DataFrame) -> pd.DataFrame:
        predict = batch["passenger_count"] >= 2
        return pd.DataFrame({"score": predict})

    return model


input_splits = [
    f"s3://anonymous@air-example-data/ursa-labs-taxi-data"
    "/downsampled_2009_full_year_data.parquet"
    f"/fe41422b01c04169af2a65a83b753e0f_{i:06d}.parquet"
    for i in range(12)
]
ds = ray.data.read_parquet(input_splits)


class CustomPredictor(Predictor):
    def __init__(self, model):
        super().__init__()
        self.model = model

    def _predict_pandas(self, batch: pd.DataFrame) -> pd.DataFrame:
        return self.model(batch)

    @classmethod
    def from_checkpoint(cls, checkpoint: Checkpoint, **kwargs) -> "CustomPredictor":
        return CustomPredictor(checkpoint.to_dict()["model"])


def preprocess(batch):
    batch["passenger_count"] -= 1.0
    return batch


model = load_trained_model()
predictor = BatchPredictor(
    checkpoint=Checkpoint.from_dict({"model": model}),
    predictor_cls=CustomPredictor,
    preprocessor=BatchMapper(preprocess, batch_format=BatchFormat.NUMPY),
)

results = predictor.predict(ds)
print(results.show(5))
