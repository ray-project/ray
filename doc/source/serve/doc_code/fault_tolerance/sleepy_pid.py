# flake8: noqa

# __start__
# File name: sleepy_pid.py

from ray import serve


@serve.deployment
class SleepyPid:
    def __init__(self):
        import time

        time.sleep(10)

    def __call__(self) -> int:
        import os

        return os.getpid()


app = SleepyPid.bind()
# __end__
