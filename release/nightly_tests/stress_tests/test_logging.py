import ray
import argparse

import random
import string

parser = argparse.ArgumentParser(prog="Stress Test - Logging")
parser.add_argument(
    "--total-num-task",
    type=int,
    help="Total number of tasks sending logs",
    required=True)
parser.add_argument(
    "--total-logs-lines",
    type=int,
    help="Total lines of logs to be sent",
    required=True)

parser.add_argument(
    "--log-line-size",
    type=int,
    help="Size of a log",
    required=True)

ray.init(address="auto")

@ray.remote
def gen_logs(log_size, log_num):
    for _ in range(log_num):
        ''.join(random.choice(letters) for i in range(10))
