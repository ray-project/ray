GRAFANA_DATASOURCE_TEMPLATE = """apiVersion: 1

datasources:
  - name: {prometheus_name}
    url: {prometheus_host}
    type: prometheus
    isDefault: true
    access: proxy
"""
