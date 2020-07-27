import json
import time

import requests

from ray import serve
from ray.serve.metric.exporter import ExporterInterface


class FileExporter(ExporterInterface):
    def __init__(self):
        self.file = open("/tmp/serve_metrics.log", "w")

    def export(self, metric_metadata, metric_batch):
        for metric_item in metric_batch:
            data = metric_metadata[metric_item.key].__dict__
            data["labels"] = metric_item.labels
            data["values"] = metric_item.value
            self.file.write(json.dumps(data))
            self.file.write("\n")
        self.file.flush()

    def inspect_metrics(self):
        return "Metric is located at /tmp/serve_metrics.log"


serve.init(metric_exporter=FileExporter)


def echo(flask_request):
    return "hello " + flask_request.args.get("name", "serve!")


serve.create_backend("hello", echo)
serve.create_endpoint("hello", backend="hello", route="/hello")

for _ in range(5):
    requests.get("http://127.0.0.1:8000/hello").text
    time.sleep(0.2)

print("Retrieving metrics from file...")
with open("/tmp/serve_metrics.log") as metric_log:
    for line in metric_log:
        print(line)

# Retrieving metrics from file...
# {"name": "backend_worker_starts",
#  "type": 1,
#  "description": "The number of time this replica workers ...",
#  "label_names": ["replica_tag"],
#  "default_labels": {"backend": "hello"}, "
#  labels": {"replica_tag": "hello#XwzPQn"},
#  "values": 1
# }
# ...
