try:
    import requests  # `requests` is not part of stdlib.
except ImportError:
    requests = None
    print("Couldn't import `requests` library. "
          "Be sure to install it on the client side.")

from ray.dashboard.metrics_exporter.schema import AuthRequest, AuthResponse
from ray.dashboard.metrics_exporter.schema import IngestRequest, IngestResponse


def authentication_request(url, cluster_id):
    auth_requeset = AuthRequest(cluster_id=cluster_id)
    response = requests.post(url, data=auth_requeset.json())
    response.raise_for_status()
    return AuthResponse.parse_obj(response.json())


def ingest_request(url, cluster_id, access_token, ray_config, node_info,
                   raylet_info, tune_info, tune_availability):
    ingest_request = IngestRequest(
        cluster_id=cluster_id,
        access_token=access_token,
        ray_config=ray_config,
        node_info=node_info,
        raylet_info=raylet_info,
        tune_info=tune_info,
        tune_availability=tune_availability)
    response = requests.post(url, data=ingest_request.json())
    response.raise_for_status()
    return IngestResponse.parse_obj(response.json())
