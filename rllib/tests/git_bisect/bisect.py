"""
This script should be used to find learning or performance regressions in RLlib.

If you think something broke after(!) some good commit C, do the following
while checked out in the current bad commit D (where D is newer than C):

$ cd ray
$ git bisect start
$ git bisect bad
$ git bisect good [the hash code of C]
$ git bisect run python debug_learning_failure_git_bisect.py [... options]


""Example of testing, whether RLlib can still learn with a certain config.

Can be used with git bisect to find the faulty commit responsible for a
learning failure. Produces an error if the given reward is not reached within
the stopping criteria (training iters or timesteps) allowing git bisect to
properly analyze and find the faulty commit.

Run as follows using a simple command line config:
$ python debug_learning_failure_git_bisect.py --config '{...}'
    --env CartPole-v0 --run PPO --stop-reward=180 --stop-iters=100

With a yaml file:
$ python debug_learning_failure_git_bisect.py -f [yaml file] --stop-reward=180
    --stop-iters=100

Within git bisect:
$ git bisect start
$ git bisect bad
$ git bisect good [some previous commit we know was good]
$ git bisect run python debug_learning_failure_git_bisect.py [... options]
""

"""
import subprocess
import os

# Assume
subprocess.run(["ci/travis/install-bazel.sh"])
os.chdir("python")
subprocess.run(["pip", "install", "-e", "."])

os.chdir("../../test")
subprocess.run(["python", "debug_learning_failure_git_bisect.py", "-f", "pong-impala-fast.yaml", "--stop-timesteps", "1000000", "--stop-reward", "300.0"])
