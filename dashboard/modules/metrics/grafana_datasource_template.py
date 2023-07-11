GRAFANA_DATASOURCE_TEMPLATE = """apiVersion: 1

datasources:
  - name: Prometheus
    url: {prometheus_host}
    type: prometheus
    isDefault: true
    access: proxy
"""
