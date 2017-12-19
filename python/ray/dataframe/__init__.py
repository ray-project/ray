from .dataframe import DataFrame
from .dataframe import from_pandas
from .dataframe import to_pandas
import ray
import pandas as pd

ray.register_custom_serializer(pd.DataFrame, use_pickle=True)
ray.register_custom_serializer(pd.core.indexes.base.Index, use_pickle=True)
