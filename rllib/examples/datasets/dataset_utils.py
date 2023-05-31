import json
import pandas as pd
import numpy as np

def convert_json_sample_batch_to_df(path: str):
    """Converts JSONified sample batch(es) to a pandas DataFrame.
    
    Args:
        path: The path to the JSON file containing the sample batches.
    
    Returns:
        A pandas DataFrame containing the sample batches.
    """
    with open(path, 'r') as file:
        json_data = file.read()
        sample_batches_str = json_data.strip().split('\n')
    sample_batches = []
    for sample_batch in sample_batches_str:
        sample_batch = json.loads(sample_batch)
        data_type = sample_batch.pop("type")
        assert data_type == "SampleBatch", "Only SampleBatch is supported."
        sample_batches.append(sample_batch)

    df = pd.DataFrame(sample_batches)
    df = df.explode(list(df.columns))
    df = df.reset_index(drop=True)
    for column in df.columns:
        df[column] = df[column].apply(lambda x: np.array(x) if isinstance(x, list) else x)
    return df
