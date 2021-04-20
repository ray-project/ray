import os

import pandas as pd
import numpy as np
import ray


def generate_data(
        num_rows,
        num_files,
        num_row_groups_per_file,
        max_row_group_skew,
        data_dir):
    results = []
    # TODO(Clark): Generate skewed row groups according to max_row_group_skew.
    for file_index, global_row_index in enumerate(
            range(0, num_rows, num_rows // num_files)):
        num_rows_in_file = min(
            num_rows // num_files, num_rows - global_row_index)
        results.append(
            generate_file.remote(
                file_index,
                global_row_index,
                num_rows_in_file,
                num_row_groups_per_file,
                data_dir))
    filenames, data_sizes = zip(*ray.get(results))
    return filenames, sum(data_sizes)


@ray.remote
def generate_file(
        file_index,
        global_row_index,
        num_rows_in_file,
        num_row_groups_per_file,
        data_dir):
    # TODO(Clark): Generate skewed row groups according to max_row_group_skew.
    # TODO(Clark): Optimize this data generation to reduce copies and
    # progressively write smaller buffers to the Parquet file.
    buffs = []
    for group_index, group_global_row_index in enumerate(
            range(
                0,
                num_rows_in_file,
                num_rows_in_file // num_row_groups_per_file)):
        num_rows_in_group = min(
            num_rows_in_file // num_row_groups_per_file,
            num_rows_in_file - group_global_row_index)
        buffs.append(
            generate_row_group(
                group_index,
                group_global_row_index,
                num_rows_in_group))
    df = pd.concat(buffs)
    data_size = df.memory_usage(deep=True).sum()
    filename = os.path.join(
        data_dir, f"input_data_{file_index}.parquet.gzip")
    df.to_parquet(
        filename,
        engine="pyarrow",
        compression="gzip",
        row_group_size=num_rows_in_file // num_row_groups_per_file)
    return filename, data_size


def generate_row_group(group_index, global_row_index, num_rows_in_group):
    buffer = {
        "key": np.array(
            range(global_row_index, global_row_index + num_rows_in_group)),
        "embeddings_name0": np.random.randint(
            0, 2385, num_rows_in_group, dtype=np.long),
        "embeddings_name1": np.random.randint(
            0, 201, num_rows_in_group, dtype=np.long),
        "embeddings_name2": np.random.randint(
            0, 201, num_rows_in_group, dtype=np.long),
        "embeddings_name3": np.random.randint(
            0, 6, num_rows_in_group, dtype=np.long),
        "embeddings_name4": np.random.randint(
            0, 19, num_rows_in_group, dtype=np.long),
        "embeddings_name5": np.random.randint(
            0, 1441, num_rows_in_group, dtype=np.long),
        "embeddings_name6": np.random.randint(
            0, 201, num_rows_in_group, dtype=np.long),
        "embeddings_name7": np.random.randint(
            0, 22, num_rows_in_group, dtype=np.long),
        "embeddings_name8": np.random.randint(
            0, 156, num_rows_in_group, dtype=np.long),
        "embeddings_name9": np.random.randint(
            0, 1216, num_rows_in_group, dtype=np.long),
        "embeddings_name10": np.random.randint(
            0, 9216, num_rows_in_group, dtype=np.long),
        "embeddings_name11": np.random.randint(
            0, 88999, num_rows_in_group, dtype=np.long),
        "embeddings_name12": np.random.randint(
            0, 941792, num_rows_in_group, dtype=np.long),
        "embeddings_name13": np.random.randint(
            0, 9405, num_rows_in_group, dtype=np.long),
        "embeddings_name14": np.random.randint(
            0, 83332, num_rows_in_group, dtype=np.long),
        "embeddings_name15": np.random.randint(
            0, 828767, num_rows_in_group, dtype=np.long),
        "embeddings_name16": np.random.randint(
            0, 945195, num_rows_in_group, dtype=np.long),
        "one_hot0": np.random.randint(0, 3, num_rows_in_group, dtype=np.long),
        "one_hot1": np.random.randint(0, 50, num_rows_in_group, dtype=np.long),
        "labels": np.random.rand(num_rows_in_group),
    }

    return pd.DataFrame(buffer)
