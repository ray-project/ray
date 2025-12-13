# client_multiple_requests.py
import random
import requests

customer_ids = ["customer_123", "customer_456", "customer_789", "customer_abc", "customer_def", "customer_hij"]

# Create a random list of 100 requests from those 3 customers
random_requests = [random.choice(customer_ids) for _ in range(100)]

# Send all 100 requests
for i, customer_id in enumerate(random_requests):
    # Generate random "live" data for each request
    live_sequence_data = [random.uniform(90, 110) for _ in range(10)]
    
    response = requests.post(
        "http://localhost:8000",
        headers={"serve_multiplexed_model_id": customer_id},
        json={"sequence_data": live_sequence_data}
    )
    forecast = response.json()["forecast"]
    print(f"Request {i+1} - {customer_id}: {forecast[:3]}...")

print(f"\nSent {len(random_requests)} requests total")
