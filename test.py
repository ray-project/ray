import ray
import requests

def format_web_url(url):
    """Format web url."""
    url = url.replace("localhost", "http://127.0.0.1")
    if not url.startswith("http://"):
        return "http://" + url
    return url

address_info = ray.init()
webui_url = format_web_url(address_info["webui_url"])

@ray.remote
def task():
    import time
    a = []
    for i in range(100):
        a.append(i)
        time.sleep(1)

@ray.remote
class Actor:
    def pid(self):
        import os
        return os.getpid()

    def task(self):
        while True:
            a = []
            for i in range(100000):
                a.append(i)
actor = Actor.remote()
actor2 = Actor.remote()
pid = ray.get(actor.pid.remote())
actor.task.remote()
actor2.task.remote()
task.remote()

from pdb import set_trace as bp
bp()