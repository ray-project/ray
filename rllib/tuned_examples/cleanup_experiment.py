"""
This script automates cleaning up a benchmark/experiment run of some algo
against some config (with possibly more than one tune trial,
e.g. torch=grid_search([True, False])).

Run `python cleanup_experiment.py --help` for more information.

Use on an input directory with trial contents e.g.:
..
IMPALA_BreakoutNoFrameskip-v4_0_use_pytorch=False_2020-05-11_10-17-54topr3h9k
IMPALA_BreakoutNoFrameskip-v4_0_use_pytorch=False_2020-05-11_13-59-35dqaetxnf
IMPALA_BreakoutNoFrameskip-v4_0_use_pytorch=False_2020-05-11_17-21-28tbhedw72
IMPALA_BreakoutNoFrameskip-v4_2_use_pytorch=True_2020-05-11_10-17-54lv20cgn_
IMPALA_BreakoutNoFrameskip-v4_2_use_pytorch=True_2020-05-11_13-59-35kwzhax_y
IMPALA_BreakoutNoFrameskip-v4_2_use_pytorch=True_2020-05-11_17-21-28a5j0s7za

Then run:
>> python cleanup_experiment.py --experiment-dir [parent dir w/ trial sub-dirs]
>>   --output-dir [your out dir] --results-filter dumb_col_2,superfluous_col3
>>   --results-max-size [max results file size in kb before(!) zipping]

The script will create one output sub-dir for each trial and only copy
the configuration and the csv results (filtered and every nth row removed
based on the given args).
"""

import argparse
import json
import os
import re
import shutil
import yaml

parser = argparse.ArgumentParser()
parser.add_argument(
    "--experiment-dir",
    type=str,
    help="Experiment dir in which all sub-runs (seeds) are "
    "located (as sub-dirs). Each sub0-run dir must contain the files: "
    "params.json and progress.csv.")
parser.add_argument(
    "--output-dir",
    type=str,
    help="The output dir, in which the cleaned up output will be placed.")
parser.add_argument(
    "--results-filter",
    type=str,
    help="comma-separated list of csv fields to exclude.",
    default="experiment_id,pid,hostname,node_ip,trial_id,hist_stats/episode_"
    "reward,hist_stats/episode_lengths,experiment_tag")
parser.add_argument(
    "--results-max-size",
    type=int,
    help="the max. size of the final results.csv file (in kb). Will erase "
    "every nth line in the original input to reach that goal. "
    "Use 0 for no limit (default=100).",
    default=100)


def process_single_run(in_dir, out_dir):
    exp_dir = os.listdir(in_dir)

    # Make sure trials dir is ok.
    assert "params.json" in exp_dir and "progress.csv" in exp_dir, \
        "params.json or progress.csv not found in {}!".format(in_dir)

    os.makedirs(out_dir, exist_ok=True)

    for file in exp_dir:
        absfile = os.path.join(in_dir, file)
        # Config file -> Convert to yaml and move to output dir.
        if file == "params.json":
            assert os.path.isfile(absfile), "{} not a file!".format(file)
            with open(absfile) as fp:
                contents = json.load(fp)
            with open(os.path.join(out_dir, "config.yaml"), "w") as fp:
                yaml.dump(contents, fp)
        # Progress csv file -> Filter out some columns, cut, and write to
        # output_dir.
        elif file == "progress.csv":
            assert os.path.isfile(absfile), "{} not a file!".format(file)
            col_idx_to_filter = []
            with open(absfile) as fp:
                # Get column names.
                col_names_orig = fp.readline().strip().split(",")
                # Split by comma (abiding to quotes), filter out
                # unwanted columns, then write to disk.
                cols_to_filter = args.results_filter.split(",")
                for i, c in enumerate(col_names_orig):
                    if c in cols_to_filter:
                        col_idx_to_filter.insert(0, i)
                col_names = col_names_orig.copy()
                for idx in col_idx_to_filter:
                    col_names.pop(idx)
                absfile_out = os.path.join(out_dir, "progress.csv")
                with open(absfile_out, "w") as out_fp:
                    print(",".join(col_names), file=out_fp)
                    while True:
                        line = fp.readline().strip()
                        if not line:
                            break
                        line = re.sub(
                            "(,{2,})",
                            lambda m: ",None" * (len(m.group()) - 1) + ",",
                            line)
                        cols = re.findall('".+?"|[^,]+', line)
                        if len(cols) != len(col_names_orig):
                            continue
                        for idx in col_idx_to_filter:
                            cols.pop(idx)
                        print(",".join(cols), file=out_fp)

            # Reduce the size of the output file if necessary.
            out_size = os.path.getsize(absfile_out)
            max_size = args.results_max_size * 1024
            if 0 < max_size < out_size:
                # Figure out roughly every which line we have to drop.
                ratio = out_size / max_size
                # If ratio > 2.0, we'll have to keep only every nth line.
                if ratio > 2.0:
                    nth = out_size // max_size
                    os.system("awk 'NR==1||NR%{}==0' {} > {}.new".format(
                        nth, absfile_out, absfile_out))
                # If ratio < 2.0 (>1.0), we'll have to drop every nth line.
                else:
                    nth = out_size // (out_size - max_size)
                    os.system("awk 'NR==1||NR%{}!=0' {} > {}.new".format(
                        nth, absfile_out, absfile_out))
                os.remove(absfile_out)
                os.rename(absfile_out + ".new", absfile_out)

            # Zip progress.csv into results.zip.
            zip_file = os.path.join(out_dir, "results.zip")
            try:
                os.remove(zip_file)
            except FileNotFoundError:
                pass
            os.system("zip -j {} {}".format(
                zip_file, os.path.join(out_dir, "progress.csv")))
            os.remove(os.path.join(out_dir, "progress.csv"))

        # TBX events file -> Move as is.
        elif re.search("^(events\\.out\\.|params\\.pkl)", file):
            assert os.path.isfile(absfile), "{} not a file!".format(file)
            shutil.copyfile(absfile, os.path.join(out_dir, file))


if __name__ == "__main__":
    args = parser.parse_args()
    exp_dir = os.listdir(args.experiment_dir)
    # Loop through all sub-directories.
    for i, sub_run in enumerate(sorted(exp_dir)):
        abspath = os.path.join(args.experiment_dir, sub_run)
        # This is a seed run.
        if os.path.isdir(abspath) and \
                re.search("^(\\w+?)_(\\w+?-v\\d+)(_\\d+)", sub_run):
            # Create meaningful output dir name:
            # [algo]_[env]_[trial #]_[trial-config]_[date YYYY-MM-DD].
            cleaned_up_out = re.sub(
                "^(\\w+?)_(\\w+?-v\\d+)(_\\d+)(_.+)?(_\\d{4}-\\d{2}-\\d{2})"
                "_\\d{2}-\\d{2}-\\w+", "{:02}_\\1_\\2\\4\\5".format(i),
                sub_run)
            # Remove superflous `env=` specifier (anv always included in name).
            cleaned_up_out = re.sub("^(.+)env=\\w+?-v\\d+,?(.+)", "\\1\\2",
                                    cleaned_up_out)
            out_path = os.path.join(args.output_dir, cleaned_up_out)
            process_single_run(abspath, out_path)
    # Done.
    print("done")
