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
  - name: Ray    # Default dashboards provided by OSS ray
    folder: Ray
    type: file
    options:
      path: {dashboard_output_folder}
"""

GRAFANA_DATASOURCE_TEMPLATE = """apiVersion: 1

datasources:
  - name: {prometheus_name}
    url: {prometheus_host}
    type: prometheus
    isDefault: true
    access: proxy
"""

PROMETHEUS_YML_TEMPLATE = """# my global config
global:
  scrape_interval: 10s # Set the scrape interval to every 10 seconds. Default is every \
1 minute.
  evaluation_interval: 10s # Evaluate rules every 10 seconds. The default is every 1 \
minute.
  # scrape_timeout is set to the global default (10s).

scrape_configs:
# Scrape from each ray node as defined in the service_discovery.json provided by ray.
- job_name: 'ray'
  file_sd_configs:
  - files:
    - '{prom_metrics_service_discovery_file_path}'
"""
