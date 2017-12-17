# from .dataframe import DataFrame
from .dataframe import *

ray.register_custom_serializer(pd.DataFrame, use_pickle=True)
ray.register_custom_serializer(pd.core.indexes.base.Index, use_pickle=True)
