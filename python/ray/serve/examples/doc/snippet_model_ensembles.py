import asyncio

import requests

from ray import serve

serve.init()


def simple_model(flask_request, *, arg=None):
    print(f"Got Python argument {arg}")
    return int(arg)


serve.create_backend("backend-1", simple_model)
serve.create_endpoint("endpoint-1", backend="backend-1")
serve.create_backend("backend-2", simple_model)
serve.create_endpoint("endpoint-2", backend="backend-2")


class AdderEnsemble:
    def __init__(self):
        self.model_1 = serve.get_handle("endpoint-1", missing_ok=True)
        self.model_2 = serve.get_handle("endpoint-2", missing_ok=True)

    async def __call__(self, flask_request):
        data = flask_request.args.get("data")
        futures = [
            self.model_1.remote(arg=data),
            self.model_2.remote(arg=data)
        ]
        results = await asyncio.gather(*futures)
        print(f"Got results {results} from models")
        return {"added": sum(results)}


serve.create_backend("ensemble-backend", AdderEnsemble)
serve.create_endpoint(
    "ensemble", backend="ensemble-backend", route="/ensemble")

resp = requests.get("http://localhost:8000/ensemble?data=100")
print(resp.json())

# (pid=15286) Got Python argument 100
# (pid=15281) Got Python argument 100
# (pid=15282) Got results [100, 100] from models
# {'added': 200}
