DASHBOARD_PROVISIONING_TEMPLATE = """
apiVersion: 1

providers:
  - name: Vorticity    # Default dashboards provided by OSS ray
    folder: Vorticity
    type: file
    options:
      path: {dashboard_output_folder}
"""
