#!/usr/bin/env python3

import subprocess
import time
from multiprocessing import Process

import ray

import psutil

ray.init()


def target():
    import os

    print(f"pid {os.getpid()}")
    time.sleep(1000)


@ray.remote
class MyActor:
    def run(self):
        print("Starting proc")
        p = Process(target=target, daemon=True)
        p.start()
        print(f"Done, p: {p}")


print("Before")
subprocess.run(
    "ps aux | grep ray:: | grep 'ray::MyActor.run' | grep -v 'grep'", shell=True
)

actor = MyActor.remote()
ray.get(actor.run.remote())

procs = [
    x for x in psutil.process_iter(["name"]) if x.name().startswith("ray::MyActor.run")
]
if procs:
    print(procs[0].ppid())

del actor

while True:
    procs = [
        x
        for x in psutil.process_iter(["name"])
        if x.name().startswith("ray::MyActor.run")
    ]
    if procs:
        print(procs[0].ppid())
    else:
        break
    time.sleep(1)

print("After")
subprocess.run(
    "ps aux | grep ray:: | grep 'ray::MyActor.run' | grep -v 'grep'", shell=True
)

# cat /proc/2611/status | grep -i ppid
# subprocess.run("ps aux | grep ray:: | grep 'ray::MyActor.run' | grep -v 'grep' | awk '{print $2}' | xargs kill -9", shell=True)
