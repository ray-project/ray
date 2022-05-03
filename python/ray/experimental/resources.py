import requests
import json
import ray._private.services as services

SESSION_DASHBOARD_URL = None


def _get_dashboard_url():
    global SESSION_DASHBOARD_URL

    if SESSION_DASHBOARD_URL is None:
        gcs_addr = services.get_ray_address_from_environment()
        dashboard_addr = services.get_dashboard_url(gcs_addr)
        if not dashboard_addr:
            return f"Cannot find the dashboard of the cluster of address {gcs_addr}."

        def format_web_url(url):
            """Format web url."""
            url = url.replace("localhost", "http://127.0.0.1")
            if not url.startswith("http://"):
                return "http://" + url
            return url

        SESSION_DASHBOARD_URL = format_web_url(dashboard_addr)
    return SESSION_DASHBOARD_URL


def path_leaf(by_node: bool):
    if by_node:
        return "/nodes"
    else:
        return "/cluster"


def get_check_and_parse_json(path: str):
    response = requests.get(_get_dashboard_url() + path)
    if response.status_code != 200:
        raise ValueError("Failed query HTTP endpoint")
    return json.loads(response.text)


def total(by_node: bool = False):
    return get_check_and_parse_json(
        "/api/experimental/resources/total" + path_leaf(by_node)
    )


def available(by_node: bool = False):
    return get_check_and_parse_json(
        "/api/experimental/resources/available" + path_leaf(by_node)
    )


def usage(by_node: bool = False):
    return get_check_and_parse_json(
        "/api/experimental/resources/usage" + path_leaf(by_node)
    )
