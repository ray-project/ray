import argparse
from collections import OrderedDict

import numpy as np
import pandas as pd

# 100 row groups.
# 4M rows/group
# ~52GB total
# 256K rows/batch.
# 4M rows/group, 256 rows/batch -> 170MB/file.

def generate(start_index, input_size):
    buffer = [OrderedDict() for _ in range(input_size)]

    for i in range(input_size):
        buffer[i]["key"] = i + start_index
        buffer[i]["embeddings"] = OrderedDict()
        buffer[i]["embeddings"]["name0"] = np.random.randint(0, 2385, dtype=np.long) 
        buffer[i]["embeddings"]["name1"] = np.random.randint(0, 201, dtype=np.long) 
        buffer[i]["embeddings"]["name2"] = np.random.randint(0, 201, dtype=np.long) 
        buffer[i]["embeddings"]["name3"] = np.random.randint(0, 6, dtype=np.long) 
        buffer[i]["embeddings"]["name4"] = np.random.randint(0, 19, dtype=np.long) 
        buffer[i]["embeddings"]["name5"] = np.random.randint(0, 1441, dtype=np.long) 
        buffer[i]["embeddings"]["name6"] = np.random.randint(0, 201, dtype=np.long) 
        buffer[i]["embeddings"]["name7"] = np.random.randint(0, 22, dtype=np.long) 
        buffer[i]["embeddings"]["name8"] = np.random.randint(0, 156, dtype=np.long) 
        buffer[i]["embeddings"]["name9"] = np.random.randint(0, 1216, dtype=np.long) 
        buffer[i]["embeddings"]["name10"] = np.random.randint(0, 9216, dtype=np.long) 
        buffer[i]["embeddings"]["name11"] = np.random.randint(0, 88999, dtype=np.long) 
        buffer[i]["embeddings"]["name12"] = np.random.randint(0, 941792, dtype=np.long) 
        buffer[i]["embeddings"]["name13"] = np.random.randint(0, 9405, dtype=np.long) 
        buffer[i]["embeddings"]["name14"] = np.random.randint(0, 83332, dtype=np.long) 
        buffer[i]["embeddings"]["name15"] = np.random.randint(0, 828767, dtype=np.long) 
        buffer[i]["embeddings"]["name16"] = np.random.randint(0, 945195, dtype=np.long) 
        buffer[i]["one_hot"] = OrderedDict()
        buffer[i]["one_hot"]["hot0"] = np.random.randint(0, 3, dtype=np.long) 
        buffer[i]["one_hot"]["hot1"] = np.random.randint(0, 50, dtype=np.long) 
        buffer[i]["labels"] = np.random.rand()

    return buffer

if __name__ == '__main__':

    # Training settings
    parser = argparse.ArgumentParser(description='PyTorch Test with Dense and Sparse Adam Optimizers')
    parser.add_argument('--num-rows-per-group', type=int, default=10, metavar='N',
                        help='input test size for testing (default: 10)')
    parser.add_argument('--num-row-groups', type=int, default=10, metavar='N',
                        help='input test size for testing (default: 10)')
    args = parser.parse_args()

    num_rows_per_group = args.num_rows_per_group
    num_row_groups = args.num_row_groups

    row_index = 0
    for i in range(num_row_groups):
        buffer = generate(row_index, num_rows_per_group)
        row_index += num_rows_per_group

        buff = pd.DataFrame(buffer)
        buff.to_parquet(f"input{i}")
