# client_concurrent_requests.py
import requests
import random
from concurrent.futures import ThreadPoolExecutor, as_completed

def send_request(user_id):
    response = requests.post(
        "http://localhost:8000",
        json={"user_id": user_id, "top_k": 3}
    )
    return user_id, response.json()

user_ids = [f"user_{random.randint(1, 1000)}" for _ in range(100)]

with ThreadPoolExecutor(max_workers=10) as executor:
    futures = [executor.submit(send_request, uid) for uid in user_ids]
    for future in as_completed(futures):
        user_id, result = future.result()
        top_items = [rec["item_id"] for rec in result["recommendations"]]
        print(f"{user_id}: {top_items}")

print("\nSent 100 concurrent requests")
