import requests

from ray import serve

client = serve.start()

# Include your class as input to the ImportedBackend constructor.
import_path = "ray.serve.utils.MockImportedBackend"
serve.create_backend("imported", import_path, "input_arg")
serve.create_endpoint("imported", backend="imported", route="/imported")

print(requests.get("http://127.0.0.1:8000/imported").text)
