import json
import logging
import requests

logger = logging.getLogger(__name__)


def post_auth(auth_url, cluster_id):
    try:
        response = requests.post(
            auth_url, data=json.dumps({
                "cluster_id": cluster_id
            }))
    except requests.exceptions.ConnectionError as e:
        logger.error("Failed to connect to the server {}.\n"
                     "Error: {}".format(auth_url, e))
        return None
    except requests.exceptions.HTTPError as e:
        logger.error("HTTP error occured while connecting to "
                     "a metrics auth server.: {}".format(e))
        return None

    if response.status_code != 200:
        logger.error("Failed to authenticate to metrics importing "
                     "server. Status code: {}".format(response.status_code))
        return None

    # authentication information
    return response.json()


def post_ingest(export_address: str, dashboard_id: str, access_token: str,
                ray_config: dict, node_info: dict, raylet_info: dict,
                tune_info: dict, tune_availability: dict):
    try:
        response = requests.post(
            export_address,
            data=json.dumps({
                "cluster_id": dashboard_id,
                "access_token": access_token,
                "ray_config": ray_config,
                "node_info": node_info,
                "raylet_info": raylet_info,
                "tune_info": tune_info,
                "tune_availability": tune_availability
            }))
    except requests.exceptions.ConnectionError as e:
        logger.error("Failed to connect to the server {} with an error: {}"
                     .format(export_address, e))
        return None
    except requests.exceptions.HTTPError as e:
        logger.error("Failed to export metrics due to "
                     "a http error: {}".format(e))
        return None

    if response.status_code != 200:
        logger.error("Failed to export metrics\n"
                     "URL: {}.\n"
                     "Status code: {}".format(export_address,
                                              response.status_code))

    return response.json()
