from ray import serve

@serve.deployment
def f(*args):
    return "Hi there!"

serve.run(f.bind())

# __prototype_code_start__
import requests

response = requests.get("http://localhost:8000/")
result = response.json()
# __prototype_code_end__

assert result == "Hi there"

# __prototype_code_start__
import requests
from requests.adapters import HTTPAdapter, Retry

session = requests.Session()

retries = Retry(
    total=5,                          # 5 retries total
    backoff_factor=0.1,               # Exponential backoff
    status_forcelist=[500, 502, 503]  # Retry on server errors
)

session.mount("http://", HTTPAdapter(max_retries=retries))

response = session.get("http://localhost:8000/", timeout=10)  # Add timeout
# __prototype_code_end__
