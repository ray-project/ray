import os

import yaml

from ray._private.authentication.authentication_token_setup import (
    _get_default_token_path,
)
from ray._private.authentication.authentication_utils import is_token_auth_enabled

GRAFANA_INI_TEMPLATE = """
[security]
allow_embedding = true

[auth.anonymous]
enabled = true
org_name = Main Org.
org_role = Viewer

[paths]
provisioning = {grafana_provisioning_folder}
"""

DASHBOARD_PROVISIONING_TEMPLATE = """
apiVersion: 1

providers:
  - name: Ray    # Default dashboards provided by OSS Ray
    folder: Ray
    type: file
    options:
      path: {dashboard_output_folder}
"""


def GRAFANA_DATASOURCE_TEMPLATE(
    prometheus_name, prometheus_host, jsonData, secureJsonData
):
    return yaml.safe_dump(
        {
            "apiVersion": 1,
            "datasources": [
                {
                    "name": prometheus_name,
                    "url": prometheus_host,
                    "type": "prometheus",
                    "isDefault": True,
                    "access": "proxy",
                    "jsonData": jsonData,
                    "secureJsonData": secureJsonData,
                }
            ],
        }
    )


PROMETHEUS_YML_TEMPLATE = """# my global config
global:
  scrape_interval: 10s # Set the scrape interval to every 10 seconds. Default is every \
1 minute.
  evaluation_interval: 10s # Evaluate rules every 10 seconds. The default is every 1 \
minute.
  # scrape_timeout is set to the global default (10s).

scrape_configs:
# Scrape from each Ray node as defined in the service_discovery.json provided by Ray.
- job_name: 'ray'
  file_sd_configs:
  - files:
    - '{prom_metrics_service_discovery_file_path}'
"""

# Indented to sit inside the 'ray' scrape job of PROMETHEUS_YML_TEMPLATE.
PROMETHEUS_AUTHORIZATION_TEMPLATE = """  authorization:
    type: Bearer
    credentials_file: '{credentials_file}'
"""


def get_prometheus_authorization_config() -> str:
    """Get the authorization block for the 'ray' scrape job.

    Prometheus reads the token from RAY_AUTH_TOKEN_PATH if set, else from
    ~/.ray/auth_token, so Ray never writes another copy of the token.

    Returns:
        The YAML block to append to PROMETHEUS_YML_TEMPLATE, or "" when token
        auth is off.

    Raises:
        ValueError: Token auth is on but the token isn't in a file.
    """
    if not is_token_auth_enabled():
        return ""
    token_path = os.path.abspath(
        os.environ.get("RAY_AUTH_TOKEN_PATH") or _get_default_token_path()
    )
    if not os.path.isfile(token_path):
        raise ValueError(
            "Token authentication is enabled, but Prometheus can only read the "
            f"token from a file and none was found at {token_path}. Put the token "
            "in a file and set RAY_AUTH_TOKEN_PATH to its path."
        )
    return PROMETHEUS_AUTHORIZATION_TEMPLATE.format(credentials_file=token_path)
