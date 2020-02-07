import time
import subprocess
from subprocess import PIPE

import requests

import ray
from ray.experimental import serve

print("Downloading load testing tool")
subprocess.call(["bash", "-c", "rm hey_linux_amd64 || true; wget https://storage.googleapis.com/hey-release/hey_linux_amd64; chmod +x hey_linux_amd64"])

ray.init(include_webui=True,webui_host='0.0.0.0')
serve.init(blocking=True)

@serve.route('/echo')
@serve.accept_batch
def echo(flask_request):
    time.sleep(0.01) # Sleep for 10ms
    ray.show_in_webui(str(serve.context.batch_size), key="Current batch size")
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

proc = subprocess.Popen(["./hey_linux_amd64", "-c", "10", "-z", "60m", "http://127.0.0.1:8000/echo"], stdout=PIPE, stderr=PIPE)
print("started load testing")
proc.wait()
out, err = proc.communicate()
print(out)
print(err)
