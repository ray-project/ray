# __client_script_begin__
import logging
import time
from datetime import datetime
import requests


APPLICATION_NAME = "text-processor-app"
DEPLOYMENT_NAME = "TextProcessor" 
AUTH_TOKEN = "YOUR_TOKEN_HERE" # Get your auth token from the Ray dashboard at http://localhost:8265
SERVE_ENDPOINT = "http://localhost:8000"
SCALING_INTERVAL = 300 # How often to check and update scaling (in seconds)

logger = logging.getLogger(__name__)


def predictive_scale(
    application_name: str,
    deployment_name: str,
    auth_token: str,
    serve_endpoint: str = "http://localhost:8000"
) -> bool:
    """Scale based on time of day and historical patterns."""
    hour = datetime.now().hour
    
    # Define scaling profile based on historical traffic patterns
    if 9 <= hour < 17:  # Business hours
        target_replicas = 10
    elif 17 <= hour < 22:  # Evening peak
        target_replicas = 15
    else:  # Off-peak hours
        target_replicas = 3
    
    url = (
        f"{serve_endpoint}/api/v1/applications/{application_name}"
        f"/deployments/{deployment_name}/scale"
    )
    
    headers = {
        "Authorization": f"Bearer {auth_token}",
        "Content-Type": "application/json"
    }
    
    try:
        response = requests.post(
            url,
            headers=headers,
            json={"target_num_replicas": target_replicas},
            timeout=10
        )
        
        if response.status_code == 200:
            logger.info(f"Successfully scaled {deployment_name} to {target_replicas} replicas, (hour: {hour})")
            return True
        else:
            logger.error(f"Failed to scale deployment: {response.status_code} - {response.text}")
            return False
            
    except requests.exceptions.RequestException as e:
        logger.error(f"Request failed: {e}")
        return False


def main():
    logger.info(
        f"Starting predictive scaling for {APPLICATION_NAME}/{DEPLOYMENT_NAME}"
    )
    
    # Continuous loop - scales based on time of day
    while True:
        predictive_scale(
            APPLICATION_NAME,
            DEPLOYMENT_NAME,
            AUTH_TOKEN,
            SERVE_ENDPOINT
        )
        time.sleep(SCALING_INTERVAL)


if __name__ == "__main__":
    main()
# __client_script_end__

