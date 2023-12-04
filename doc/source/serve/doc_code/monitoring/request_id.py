from ray import serve

import requests


@serve.deployment
class Model:
    def __call__(self) -> int:
        return 1


serve.run(Model.bind())
resp = requests.get("http://localhost:8000", headers={"X-Request-ID": "123-234"})

print(resp.headers["X-Request-ID"])
