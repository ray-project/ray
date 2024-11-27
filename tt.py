import ray
import pandas as pd
import numpy as np

# Initialize Ray
ray.init(ignore_reinit_error=True)

# Create a sample pandas DataFrame
df = pd.DataFrame([[6, 2], [1, 5], [4, 3]], columns=["x", "y"])

# Convert the pandas DataFrame to a Ray dataset
ds = ray.data.from_pandas(df)

# Sort the dataset by column 'x' (or both columns as an example)
sorted_ds = ds.sort(key=None)

# Take all sorted data
sorted_data = sorted_ds.take_all()

# Print sorted data
print(sorted_data)


import pandas as pd

# Sample dataset (same as your previous example)
df = pd.DataFrame([[6, 2], [1, 5], [4, 3]], columns=["x", "y"])

# Attempt to sort with key=None (will raise an error)
sorted_df = df.sort_values(by=None)
