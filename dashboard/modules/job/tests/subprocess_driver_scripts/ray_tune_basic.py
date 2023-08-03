"""Tests that Ray Tune works with the working_dir set in Jobs.

Ray Tune internally sets environment variables using runtime_env.
If the inherited internal runtime environment overwrites the working_dir
from jobs with an empty working_dir, this test will fail.  See #25484"""
from ray_tune_dependency import foo
from ray import tune


def objective(*args):
    foo()


tune.run(objective)
