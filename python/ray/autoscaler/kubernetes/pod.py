from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

default_pod_config = {
    "apiVersion": "v1",
    "kind": "Pod",
    "metadata": {
        "namespace": "ray",
        "generateName": "ray-worker-"
    },
    "spec": {
        "restartPolicy": "Never",
        "affinity": None,
        "volumes": [{
            "name": "dshm",
            "emptyDir": {
                "medium": "Memory"
            }
        }],
        "containers": [{
            "name": "ray-worker",
            "image": "eoakes/ray-test",
            "command": ["/bin/bash", "-c", "--"],
            "args": [
                "cd ray && git fetch && git checkout k8s && git reset --hard origin/k8s && cd .. && apt-get install -y rsync && trap : TERM INT; sleep infinity & wait;"
            ],
            "ports": [{
                "containerPort": 12345
            }, {
                "containerPort": 12346
            }],
            "volumeMounts": [{
                "mountPath": "/dev/shm",
                "name": "dshm"
            }],
            "env": [{
                "name": "LC_ALL",
                "value": "C.UTF-8"
            }, {
                "name": "LANG",
                "value": "C.UTF-8"
            }, {
                "name": "MY_CPU_LIMIT",
                "valueFrom": {
                    "resourceFieldRef": {
                        "containerName": "ray-worker",
                        "resource": "limits.cpu"
                    }
                }
            }],
            "resources": {
                "requests": {
                    "cpu": "100m",
                    "memory": "1Gi"
                }
            }
        }]
    }
}
