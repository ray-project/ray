# __client_script_begin__
import logging
import time
from datetime import datetime
import requests

APPLICATION_NAME = "text-processor-app"
DEPLOYMENT_NAME = "TextProcessor"
AUTH_TOKEN = "YOUR_TOKEN_HERE"  # Get from Ray dashboard at http://localhost:8265
SERVE_ENDPOINT = "http://localhost:8000"
SCALING_INTERVAL = 300  # Check every 5 minutes

logger = logging.getLogger(__name__)


def get_current_replicas(app_name: str, deployment_name: str, token: str) -> int:
    """Get current replica count. Returns -1 on error.
    
    Response schema: https://docs.ray.io/en/latest/serve/api/doc/ray.serve.schema.ServeInstanceDetails.html
    """
    try:
        resp = requests.get(
            f"{SERVE_ENDPOINT}/api/v1/applications",
            headers={"Authorization": f"Bearer {token}"},
            timeout=10
        )
        if resp.status_code != 200:
            logger.error(f"Failed to get applications: {resp.status_code}")
            return -1
            
        apps = resp.json().get("applications", {})
        if app_name not in apps:
            logger.error(f"Application {app_name} not found")
            return -1
            
        for deployment in apps[app_name].get("deployments", []):
            if deployment["name"] == deployment_name:
                return deployment["target_num_replicas"]
                
        logger.error(f"Deployment {deployment_name} not found")
        return -1
    except requests.exceptions.RequestException as e:
        logger.error(f"Request failed: {e}")
        return -1


def scale_deployment(app_name: str, deployment_name: str, token: str):
    """Scale deployment based on time of day."""
    hour = datetime.now().hour
    current = get_current_replicas(app_name, deployment_name, token)
    target = 10 if 9 <= hour < 17 else 3  # Peak hours: 9am-5pm
    
    delta = target - current
    if delta == 0:
        logger.info(f"Already at target ({current} replicas)")
        return
    
    action = "Adding" if delta > 0 else "Removing"
    logger.info(f"{action} {abs(delta)} replicas ({current} -> {target})")
    
    try:
        resp = requests.post(
            f"{SERVE_ENDPOINT}/api/v1/applications/{app_name}/deployments/{deployment_name}/scale",
            headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
            json={"target_num_replicas": target},
            timeout=10
        )
        if resp.status_code == 200:
            logger.info("Successfully scaled deployment")
        else:
            logger.error(f"Scale failed: {resp.status_code} - {resp.text}")
    except requests.exceptions.RequestException as e:
        logger.error(f"Request failed: {e}")


if __name__ == "__main__":
    logger.info(f"Starting predictive scaling for {APPLICATION_NAME}/{DEPLOYMENT_NAME}")
    while True:
        scale_deployment(APPLICATION_NAME, DEPLOYMENT_NAME, AUTH_TOKEN)
        time.sleep(SCALING_INTERVAL)
# __client_script_end__
