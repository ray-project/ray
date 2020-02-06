import time
import subprocess

import requests

from ray.experimental import serve

print("Downloading load testing tool")
subprocess.call(["wget", "https://storage.googleapis.com/hey-release/hey_linux_amd64"])

serve.init(blocking=True)

@serve.route('/echo')
@serve.accept_batch
def echo(flask_request):
    time.sleep(0.01) # Sleep for 10ms
    return ['hi {}'.format(i) for i in range(serve.context.batch_size)]

config = serve.get_backend_config("echo:v0")
config.num_replicas = 6
config.max_batch_size = 16
serve.set_backend_config("echo:v0", config)

print("Warming up")
for _ in range(5):
    resp = requests.get("http://127.0.0.1:8000/echo").json()
    print(resp)
    time.sleep(0.5)

proc = subprocess.Popen(["hey_linux_amd64", "-c", "10", "-z", "60m", "http://127.0.0.1:8000/echo"])
print("started load testing")
proc.wait()
print(proc.communicate())
