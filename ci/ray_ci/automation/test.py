import requests
import os
NAMESPACE = "rayproject"
REPOSITORY = "ray"

def _get_docker_token():
    """
    Retrieve Docker token.
    """
    service, scope = "registry.docker.io", f"repository:{NAMESPACE}/{REPOSITORY}:pull"
    auth_url = f"https://auth.docker.io/token?service={service}&scope={scope}"
    response = requests.get(auth_url)
    token = response.json().get("token", None)
    return token

def _get_auth_token_docker_hub():
    username = os.environ["DOCKER_HUB_USERNAME"]
    password = os.environ["DOCKER_HUB_PASSWORD"]
    url = "https://hub.docker.com/v2/users/login"
    params = {
        "username": username,
        "password": password,
    }
    headers = {"Content-Type": "application/json"}
    response = requests.post(url, headers=headers, json=params)
    return response.json().get("token", None)

def query_tags_from_docker_registry():
    token = _get_auth_token_docker_hub()
    token = _get_docker_token()
    url = f"https://index.docker.io/v2/{NAMESPACE}/{REPOSITORY}/tags/list?n=10"
    headers = {
        f"Authorization": f"Bearer {token}",
    }
    response = requests.get(url, headers=headers)
    print(response.json())
    return response.json()

query_tags_from_docker_registry()
