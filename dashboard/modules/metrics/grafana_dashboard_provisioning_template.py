DASHBOARD_PROVISIONING_TEMPLATE = """
apiVersion: 1

providers:
  - name: Ray    # Default dashboards provided by OSS ray
    folder: Ray
    type: file
    options:
      path: {dashboard_output_folder}
"""
