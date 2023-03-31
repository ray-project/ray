#!/usr/bin/env python3

import ray
import time
from multiprocessing import Process
import subprocess

def target():
    import os
    print(f'pid {os.getpid()}')
    time.sleep(1000)

@ray.remote
class MyActor:

    def run(self):
        print("Starting proc")
        p = Process(target=target, daemon=True)
        p.start()
        print("Done")

print('Before')
subprocess.run("ps aux | grep ray:: | grep 'ray::MyActor.run' | grep -v 'bash'", shell=True)
subprocess.run("ps aux | grep ray:: | grep 'ray::MyActor.run' | grep -v 'bash' | awk '{print $2}' | xargs kill -9", shell=True)

actor = MyActor.remote()
ray.get(actor.run.remote())

print('After')
subprocess.run("ps aux | grep ray:: | grep 'ray::MyActor.run' | grep -v 'grep'", shell=True)
subprocess.run("ps aux | grep ray:: | grep 'ray::MyActor.run' | grep -v 'grep' | awk '{print $2}' | xargs kill -9", shell=True)
