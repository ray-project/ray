import json

import requests
import os

httpPrefix = 'http://'
trainerStartUrl = '/api/v1/trainer/create'
trainerStopUrl = '/api/v1/trainer/delete'
workerStartUrl = '/api/v1/worker/create'
workerStopUrl = '/api/v1/worker/delete'


class OperatorClient:
    def __init__(self, address):
        self._address = address
        self._proxy_name = os.getenv("PROXY_NAME")
        self._namespace = os.getenv("NAMESPACE")

    def start_trainer(self, grpc_port, proxy_address, task_id, spec):
        # Make a POST request with JSON data
        # {
        #     "name": "trainer-test",
        #     "namespace": "ray-automl-system",
        #     "proxyName": "proxy-sample",
        #     "startParams": {
        #         "host-name": "$MY_POD_IP",
        #         "task-id": "0"
        #     }
        # }
        data = {
            'name': f"{'trainer'}-{task_id}",
            'namespace': self._namespace,
            'proxyName': self._proxy_name,
            'startParams': {
                'grpc-port': grpc_port,
                'task-id': str(task_id)
            }
        }

        response = requests.post(httpPrefix + self._address + trainerStartUrl, json=data)

        # Check the status code of the response
        if response.status_code == 200:
            # Print the response content
            print(response.json())
        else:
            # Handle the error
            print('Error:', response.status_code)

        return json.loads(response.json()["data"])["metadata"]["name"]

    def stop_trainer(self, trainer_id):
        data = {
            'name': trainer_id,
            'namespace': self._namespace,
            'proxyName': self._proxy_name
        }

        response = requests.post(httpPrefix + self._address + trainerStopUrl, json=data)

        # Check the status code of the response
        if response.status_code == 200:
            # Print the response content
            print(response.json())
        else:
            # Handle the error
            print('Error:', response.status_code)
        return True

    def start_worker_group(self, trainer_address, number, specs):
        # Make a POST request with JSON data
        # {
        #     "namespace": "ray-automl-system",
        #     "trainerAddress": "trainer-test-svc.ray-automl-system.svc.cluster.local",
        #     "workers": {
        #         "group1": {
        #             "replicas": "1"
        #         }
        #     }
        # }
        data = {
            'namespace': self._namespace,
            'trainerAddress': trainer_address,
            'workers': {
                'group1': {
                    "replicas": str(number)
                }
            }
        }

        response = requests.post(httpPrefix + self._address + workerStartUrl, json=data)

        # Check the status code of the response
        if response.status_code == 200:
            # Print the response content
            print(response.json())
        else:
            # Handle the error
            print('Error:', response.status_code)

        return json.loads(response.json()["data"])['group_id'], json.loads(response.json()["data"])['worker_ids']

    def stop_worker_group(self, group_id):
        # Make a POST request with JSON data
        # {
        #     "namespace": "ray-automl-system",
        #     "trainerAddress": "trainer-test-svc.ray-automl-system.svc.cluster.local",
        #     "workers": {
        #         "group1": {
        #             "replicas": "1"
        #         }
        #     }
        # }
        data = {
            'namespace': self._namespace,
            'workers': {
                group_id: {}
            }
        }

        response = requests.post(httpPrefix + self._address + workerStopUrl, json=data)

        # Check the status code of the response
        if response.status_code == 200:
            # Print the response content
            print(response.json())
        else:
            # Handle the error
            print('Error:', response.status_code)

        return True
