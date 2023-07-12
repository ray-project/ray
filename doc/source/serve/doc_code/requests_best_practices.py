# flake8: noqa
# fmt: off


from ray import serve


@serve.deployment
def f(*args):
    return "Hi there!"


serve.run(f.bind())

# __prototype_code_start__
import requests

response = requests.get("http://localhost:8000/")
result = response.text
# __prototype_code_end__

assert result == "Hi there!"

# __production_code_start__
import requests
from requests.adapters import HTTPAdapter, Retry

session = requests.Session()

retries = Retry(
    total=5,  # 5 retries total
    backoff_factor=1,  # Exponential backoff
    status_forcelist=[  # Retry on server errors
        500,
        501,
        502,
        503,
        504,
    ],
)

session.mount("http://", HTTPAdapter(max_retries=retries))

response = session.get("http://localhost:8000/", timeout=10)  # Add timeout
result = response.text
# __production_code_end__

assert result == "Hi there!"
