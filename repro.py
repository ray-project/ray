import ray
from ray.data.preprocessors import Concatenator
import pandas as pd

df = pd.DataFrame({"X0": [0, 0, 0], "Y": [0, 0, 0]})
ds = ray.data.from_pandas(df)
preprocessor = Concatenator(exclude=["Y"])
print(ds)
print(preprocessor.fit_transform(ds))
print(preprocessor.fit_transform(ds).to_pandas())
new_ds = preprocessor.fit_transform(ds).to_pandas()