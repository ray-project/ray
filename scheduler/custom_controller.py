import requests
import copy
from custom_resource import Resource
import time
import ray
from pprint import pprint
from ray.util.scheduling_strategies import (
    In,
    NotIn,
    Exists,
    DoesNotExist,
    NodeLabelSchedulingStrategy,
)

class Controller():
    def __init__(self):
       pass

    def start_loop(self):
        while True:
            data = requests.get("http://localhost:8000/get").json()
            instance = Resource(**data)
            self.reconcile(instance)
            self.update_status(instance)
            # Use rate limiter later
            time.sleep(1)

    def reconcile(self, instance):
        print("reconcile: ", instance)
        if instance.spec == {}:
            return

        # schedule to this task to a node
        if instance.status["assign_node"] is None:
            node_id = self.schedule(instance)
            print("reconcile: assign to node", node_id)
            instance.status["assign_node"] = node_id
            return

        # bind label to node
        elif instance.status["bind_label"] is None:
            print("reconcile: bind label to node", instance.status["assign_node"], instance.spec["working_dir"])
            bind_status = self.bind(instance.status["assign_node"], instance.spec["working_dir"])
            instance.status["bind_label"] = bind_status
            return
        
        # Check if binded
        else:
            for node in self.get_cluster_info():
                if node["NodeID"] == instance.status["assign_node"]:
                    if instance.spec["working_dir"] in node["Labels"]:
                        print("reconcile: binded", instance.status["assign_node"], instance.spec["working_dir"])
                        instance.status["bind_label"] = "binded"
                        return

    
    def bind(self, node_id, label):
        @ray.remote
        def bind_label():
            ray.get_runtime_context().set_label({label: label})
            return "binding"

        obj_id = bind_label.options(
            scheduling_strategy=NodeLabelSchedulingStrategy(
                hard={"ray.io/node_id": In(node_id)}
            )
        ).remote()

        return ray.get(obj_id)

    def schedule(self, instance):
        nodes = self.get_cluster_info()
        pprint(ray.nodes())
        return nodes[0]["NodeID"]
    

    def get_cluster_info(self):
        nodes = ray.nodes()
        return nodes
    
    def update_status(self, instance):
        requests.post("http://localhost:8000/update-status", json=instance.to_dict())

        

       
            