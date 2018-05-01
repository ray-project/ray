import argparse
import time
import sys
import numpy as np

parser = argparse.ArgumentParser(
    description='Run a performance test on a dataframe function.')
parser.add_argument('data', help='datafile to load in')
parser.add_argument('test_op', help='operation to evaluate')
parser.add_argument('--ray', dest='is_ray', action='store_true',
                    help='test performance with ray')
parser.add_argument('--nrows', dest='nrows', action='store', default=8)
parser.add_argument('--ncols', dest='ncols', action='store', default=8)

args = parser.parse_args()

def waitall(this_df):
    if args.is_ray:
        parts = this_df._block_partitions.flatten().tolist()
        ray.wait(parts, len(parts))

def waitall_parts(parts):
    parts = parts.flatten().tolist()
    ray.wait(parts, len(parts))

datafile = args.data
test_op = args.test_op

if args.is_ray:
    sys.stdout = None
    import ray.dataframe as pd
    import ray
    sys.stdout = sys.__stdout__
    pd.set_nrowpartitions(int(args.nrows))
    pd.set_ncolpartitions(int(args.ncols))
    frame = "ray"
else:
    import pandas as pd
    frame = "pandas"

df = pd.read_csv(datafile)
waitall(df)

start = time.time()
try:
    res_df = eval(test_op)
    if isinstance(res_df, pd.DataFrame):
        waitall(res_df)
    # res_parts = np.array([pd.utils._map_partitions(lambda df: df.isna(), block) for block in df._block_partitions])
    # waitall_parts(res_parts)
except KeyboardInterrupt:
    print("\nABORTED!", end=' ')
finally:
    end = time.time()
    print("elapsed seconds: {}".format(end - start))

# parts_list = ray.get(df._block_partitions.flatten().tolist())
# start2 = original = time.time()
# for i in range(0, len(parts_list), 8):
#     [k.isna() for k in parts_list[i:i+8]]
#     end2 = time.time()
#     print("manual pandas, {}-th partition: {}".format(i+8, end2 - start2))
#     start2 = end2
# end2 = time.time()
# print("elapsed2: {}".format(end2 - original))
# 
