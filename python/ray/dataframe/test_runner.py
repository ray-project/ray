import argparse
import subprocess
import os
import time
import re
import numpy as np
import sys
import scipy.io

parser = argparse.ArgumentParser(
    description='Run performance tests on a dataframe function.')
parser.add_argument('data', help='datafile to load in')
parser.add_argument('test_op', help='operation to evaluate')
parser.add_argument('--nrows', dest='nrows', default="8")
parser.add_argument('--ncols', dest='ncols', default="8")
parser.add_argument('--niters', dest='niters', type=int, default=10)
parser.add_argument('args', nargs=argparse.REMAINDER)

args = parser.parse_args()
nrows = args.nrows.split(',')
ncols = args.ncols.split(',')

means = np.full((len(nrows), len(ncols)), np.nan)
stddevs = np.full((len(nrows), len(ncols)), np.nan)
all_results = np.empty((len(nrows), len(ncols), args.niters))
nrows_arr = np.array([int(x) for x in nrows])
ncols_arr = np.array([int(x) for x in ncols])

for i, nrow in enumerate(nrows):
    for j, ncol in enumerate(ncols):
        print("Testing \"{}\" with {} rows and {} cols".format(args.test_op, nrow, ncol))
        results = np.full(args.niters, np.nan)

        extra_args = ['--nrows', nrow, '--ncols', ncol]

        for k in range(args.niters):
            cp = subprocess.run(['python', 'ray_run.py', args.data, args.test_op] + args.args + extra_args,
                                stderr=subprocess.DEVNULL, stdout=subprocess.PIPE)
            output = cp.stdout.decode('utf-8')
            print(output, end='')
            match = re.search('elapsed seconds: ([+-]?([0-9]*[.])?[0-9]+)', output)
            if match:
                result = match.group(1)
                results[k] = float(result)
            time.sleep(1)

        mean = np.nanmean(results)
        stddev = np.nanstd(results)
        means[i, j] = mean
        stddevs[i, j] = stddev
        all_results[i, j, :] = results

        print('mean:', mean)
        print('stddev:', stddev)

fname_base = "{}_{}_{}_res".format(args.data,
                                   args.test_op.replace(' ', '_'),
                                   time.strftime("%Y%m%d_%H:%M", time.gmtime()))
items = {'mean': means, 'stddev': stddevs, 'results': all_results, 'nrows': nrows_arr, 'ncols': ncols_arr}

scipy.io.savemat(fname_base, items)


