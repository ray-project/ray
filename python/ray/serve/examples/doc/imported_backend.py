import requests

from ray import serve

client = serve.start()

import_path = "my_module.MyFuncOrClass"
serve.create_backend("imported", import_path, "input_arg")
serve.create_endpoint("imported", backend="imported", route="/imported")

print(requests.get("http://127.0.0.1:8000/imported").text)
