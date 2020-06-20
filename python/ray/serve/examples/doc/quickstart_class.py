from ray import serve
import requests

serve.init()


class Counter:
    def __init__(self):
        self.count = 0

    def __call__(self, flask_request):
        return {"current_counter": self.count}


serve.create_backend("counter", Counter)
serve.create_endpoint("counter", backend="counter", route="/counter")

requests.get("http://127.0.0.1:8000/counter").json()
# > {"current_counter": self.count}
