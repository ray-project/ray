"""
This module can be run as a python script when track is installed like so:

python -m trials --local_dir ~/track/myproject

The module prints the trial UUIDs that are available in the specified local
or remote directories.

By default, this prints all available trials sorted by start time, but just
the trial_ids. We allow rudimentary filtering. For example, suppose all
trials in the default local directory (see track.autodetect.dfl_local_dir) are
summarized by

$ python -m track.trials
trial_id    start_time                 git_pretty
8424fb387a 2018-06-28 11:17:28.752259  c568050 Switch to track logging (#13)
6187e7bc7a 2018-06-27 18:27:11.635714  c568050 Switch to track logging (#13)
14b0ed447a 2018-06-27 18:25:02.718893  c568050 Switch to track logging (#13)

The default prints the above columns sorted by start time. We can ask for
parameters on the command line.

$ python -m track.trials learning_rate
learning_rate
0.1
0.1
0.1

$ python -m track.trials learning_rate "start_time>2018-06-28"
learning_rate start_time
0.1           2018-06-28 11:17:28.752259

In other words, only included columns are printed.
"""

from absl import flags
from absl import app
import pandas as pd

from . import Project

flags.DEFINE_string("local_dir", None,
                          "the local directory where trials are stored "
                          "for default behavior see "
                          "track.autodetect.dfl_local_dir")
flags.DEFINE_string("remote_dir", None,
                          "the remote directory where trials are stored")

def compare(c, l, r):
    if c == '=':
        return l == r
    elif c == '<':
        return l < r
    elif c == '>':
        return l > r
    else:
        raise ValueError('unknown operator ' + c)

def _drop_first_two_words(sentence):
    remain = sentence.partition(' ')[2]
    return remain.partition(' ')[2]

def _parse_pandas(lit):
    df = pd.read_json('{{"0": "{}"}}'.format(lit), typ="series")
    return df[0]

def _main(argv):
    proj = Project(flags.FLAGS.local_dir, flags.FLAGS.remote_dir)
    dt_cols = ["start_time", "end_time"]
    formatter = lambda x: x.strftime("%Y-%m-%d %H:%M.%S")
    argv = argv[1:]
    cols = []
    df = proj.ids
    conds = '<>='
    for arg in argv:
        if not any(c in arg for c in conds):
            cols.append(arg)
            continue
        for c in conds:
            if c not in arg:
                continue
            col, lit = arg.split(c)
            cols.append(col)
            lit = _parse_pandas(lit)
            df = df[compare(c, df[col], lit)]
    df = df.sort_values("start_time", ascending=False)
    # just the flags
    df["invocation"] = df["invocation"].map(_drop_first_two_words)
    if not cols:
        cols = ["trial_id", "start_time", "git_pretty"]
    print(df[cols].to_string(
        index=False, justify='left',
        formatters={x: formatter for x in dt_cols}))


if __name__ == '__main__':
    app.run(_main)
