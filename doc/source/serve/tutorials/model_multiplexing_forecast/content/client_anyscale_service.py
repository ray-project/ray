# client_anyscale_service.py
import requests

ENDPOINT = "<YOUR-ENDPOINT>"  # From the deployment output
TOKEN = "<YOUR-TOKEN>"  # From the deployment output

response = requests.post(
    ENDPOINT,
    headers={
        "Authorization": f"Bearer {TOKEN}",
        "serve_multiplexed_model_id": "customer_123"
    },
    json={"sequence_data": [100, 102, 98, 105, 110]}
)

print(response.json())
