import argparse
import time

parser = argparse.ArgumentParser(
    description='Run a performance test on a dataframe function.')
parser.add_argument('data', help='datafile to load in')
parser.add_argument('test_op', help='operation to evaluate')
parser.add_argument('--ray', dest='is_ray', action='store_true',
                    help='test performance with ray')
parser.add_argument('--nrows', dest='nrows', action='store')
parser.add_argument('--ncols', dest='ncols', action='store')

args = parser.parse_args()

def waitall(this_df):
    if args.is_ray:
        parts = this_df._block_partitions.flatten().tolist()
        ray.wait(parts, len(parts))

datafile = args.data
test_op = args.test_op

if args.is_ray:
    import ray.dataframe as pd
    import ray
else:
    import pandas as pd

df = pd.read_csv(datafile)
waitall(df)

start = time.time()
try:
    res_df = eval(test_op)
    if isinstance(res_df, pd.DataFrame):
        waitall(res_df)
except KeyboardInterrupt:
    print("\nABORTED!", end=' ')
finally:
    end = time.time()
    print("elapsed seconds: {}".format(end - start))
