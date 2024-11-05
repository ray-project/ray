import pandas as pd
import pyarrow as pa

import ray

df = pd.DataFrame({"a": [0]})
ds1 = ray.data.from_pandas(df)


table = pa.Table.from_pandas(df)
ds2 = ray.data.from_arrow(table)


ds1.union(ds2).to_pandas()
