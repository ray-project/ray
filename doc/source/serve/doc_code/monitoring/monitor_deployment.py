# flake8: noqa
# fmt: off

# __monitor_start__
from typing import List, Dict

from ray import serve
from ray.serve.schema import ServeStatus, ApplicationStatusOverview


@serve.deployment
def get_healthy_apps() -> List[str]:
    serve_status: ServeStatus = serve.status()
    app_statuses: Dict[str, ApplicationStatusOverview] = serve_status.applications

    running_apps = []
    for app_name, app_status in app_statuses.items():
        if app_status.status == "RUNNING":
            running_apps.append(app_name)

    return running_apps


monitoring_app = get_healthy_apps.bind()
# __monitor_end__

serve.run(monitoring_app, name="monitor")

import requests

resp = requests.get("http://localhost:8000/")

assert requests.get("http://localhost:8000/").json() == ["monitor"]
