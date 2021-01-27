import requests

from ray import serve
from ray.serve.backends import ImportedBackend

client = serve.start()

# Include your class as input to the ImportedBackend constructor.
backend_class = ImportedBackend("ray.serve.utils.MockImportedBackend")
client.create_backend("imported", backend_class, "input_arg")
client.create_endpoint("imported", backend="imported", route="/imported")

print(requests.get("http://127.0.0.1:8000/imported").text)
