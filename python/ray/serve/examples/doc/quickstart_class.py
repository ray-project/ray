from ray import serve
import requests

serve.init()


@serve.route("/counter")
class Counter:
    def __init__(self):
        self.count = 0

    def __call__(self, flask_request):
        return {"current_counter": self.count}


requests.get("http://127.0.0.1:8000/counter").json()
# > {"current_counter": self.count}