# client_anyscale_service.py
import requests

ENDPOINT = "<YOUR-ENDPOINT>"  # From the deployment output
TOKEN = "<YOUR-TOKEN>"  # From the deployment output

response = requests.post(
    ENDPOINT,
    headers={"Authorization": f"Bearer {TOKEN}"},
    json={
        "user_id": "user_42",
        "top_k": 5
    }
)

print(response.json())
