import pandas as pd
import ray
from ray.train.batch_predictor import BatchPredictor

# Create a batch predictor that always returns `42` for each input.
batch_pred = BatchPredictor.from_pandas_udf(
    lambda data: pd.DataFrame({"a": [42] * len(data)}))

# Create a dummy dataset.
ds = ray.data.range_tensor(1000, parallelism=4)

# Setup a prediction pipeline.
print(batch_pred.predict_pipelined(ds, blocks_per_window=1))