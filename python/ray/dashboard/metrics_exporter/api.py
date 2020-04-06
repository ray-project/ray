import json

try:
    import requests  # `requests` is not part of stdlib.
except ImportError:
    requests = None
    print("Couldn't import `requests` library. "
          "Be sure to install it on the client side.")

from ray.dashboard.metrics_exporter.schema import AuthRequest, AuthResponse
from ray.dashboard.metrics_exporter.schema import IngestRequest, IngestResponse


def authentication_request(url, cluster_id) -> AuthResponse:
    auth_requeset = AuthRequest(cluster_id=cluster_id)
    response = requests.post(url, data=auth_requeset.json())
    response.raise_for_status()
    return AuthResponse.parse_obj(json.loads(response.json()))


def ingest_request(url, access_token, ray_config, node_info, raylet_info,
                   tune_info, tune_availability) -> IngestResponse:
    ingest_request = IngestRequest(
        ray_config=ray_config,
        node_info=node_info,
        raylet_info=raylet_info,
        tune_info=tune_info,
        tune_availability=tune_availability)
    response = requests.post(
        url,
        headers={
            "Content-Type": "application/json",
            "Authorization": "Bearer {access_token}".format(
                access_token=access_token)
        },
        data=ingest_request.json())
    response.raise_for_status()
    return IngestResponse.parse_obj(json.loads(response.json()))
