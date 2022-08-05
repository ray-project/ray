from ray import serve

import time
import requests


@serve.deployment
def sleeper():
    time.sleep(1)


s = sleeper.bind()

serve.run(s)

while True:
    requests.get("http://localhost:8000/")
