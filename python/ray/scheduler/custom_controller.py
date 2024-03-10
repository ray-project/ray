import requests
import copy
from custom_resource import Task
import time
import ray
import boto3
from boto3.s3.transfer import TransferConfig
from pprint import pprint
from ray.util.scheduling_strategies import (
    In,
    NotIn,
    Exists,
    DoesNotExist,
    NodeLabelSchedulingStrategy,
)
from ray.util.state import list_tasks
from scheduler_constant import *
from ray.util.state import get_task
import random
import os
import subprocess

class Controller():
    def __init__(self):
       self.indicator = 0

    def start_loop(self):
        while True:
            
            data = requests.get("http://localhost:8000/get").json()
            user_task = Task(**data)
            if user_task.spec == {}:
                continue
            
            self.reconcile(user_task)
            self.update_status(user_task)
            # print("reconcile: ", user_task)
            
            # Use rate limiter later
            # time.sleep(0.001)

    def reconcile(self, user_task):
        # print("reconcile: ", user_task.spec[USER_TASK_ID])
        # print("s3 parameter: ",user_task.status[ASSIGN_NODE], user_task.spec[HPC_DIR],user_task.spec['s3'],user_task.spec[BUCKET_NAME])
        # print("s3 Object Key:",user_task.spec[OBJECT_KEY])
        # schedule to this task to a node

        print("[RECONCILE TASK] user task:", user_task)
        # if user_task.status[BIND_TASK_STATUS] is not None:
        #     task_status = get_task(user_task.status[BIND_TASK_ID])
        #     if task_status == None:
        #         return
        #     print("checking ray task status: ", task_status)


        if user_task.status[ASSIGN_NODE] is None:
            node_id = self.schedule(user_task)
            if node_id is None:
                return
            user_task.status[ASSIGN_NODE] = node_id
            user_task.status[USER_TASK_STATUS] = PENDING
            # print("reconcile: assign to node", user_task.spec[USER_TASK_ID], node_id)
            return

        # send data and bind label to node
        elif user_task.status[BIND_TASK_STATUS] is None:
            id = self.bind_label_and_send_data(user_task.status[ASSIGN_NODE], user_task.spec[HPC_DIR],user_task.spec['s3'],user_task.spec[BUCKET_NAME],user_task.spec[OBJECT_KEY])
            user_task.status[BIND_TASK_ID] = id
            user_task.status[BIND_TASK_STATUS] = RUNNING
            # print("reconcile: send data and bind label to node", user_task.spec[USER_TASK_ID], user_task.spec[HPC_DIR])
            return
        
        # Check if data is sent and label is binded

        elif user_task.status[BIND_TASK_STATUS] == RUNNING:
            task_status = get_task(user_task.status[BIND_TASK_ID])
            if task_status == None:
                return
            print("[BIND TASK RUNNING] user task status: ", user_task)
            print("[BIND TASK RUNNING] ray task status: ", task_status)
            # print(task_status)
            # The task_status is retrieved from the Ray API, and the end/start time may sometimes not be updated yet even though the state is FINISHED.
            if task_status[STATE] == FINISHED and task_status[END_TIME] != None and task_status[START_TIME] != None:
                user_task.status[BIND_TASK_START_TIME] = task_status[START_TIME]
                user_task.status[BIND_TASK_END_TIME] = task_status[END_TIME]
                user_task.status[BIND_TASK_STATUS] = FINISHED
                user_task.status[BIND_TASK_DURATION] = task_status[END_TIME] - task_status[START_TIME]
                # print("reconcile: send data and bind label finished", user_task.spec[USER_TASK_ID], user_task.spec[HPC_DIR])
                # print("reconcile: binding task duration", user_task.status[BIND_TASK_DURATION])
            return
        
        # Check if user_task is finished
        elif user_task.status[BIND_TASK_STATUS] == FINISHED:
            task_status = get_task(user_task.spec[USER_TASK_ID])
            if task_status == None:
                return
            print("[BIND TASK FINISHED] user task status: ", user_task)
            print("[BIND TASK FINISHED] ray task status: ", task_status)
            # The task_status is retrieved from the Ray API, and the end/start time may sometimes not be updated yet even though the state is FINISHED.
            if task_status[STATE] == RUNNING and task_status[START_TIME] != None:
                user_task.status[USER_TASK_START_TIME] = task_status[START_TIME]
                user_task.status[USER_TASK_STATUS] = RUNNING
                # print("reconcile: user_task running", user_task.spec[USER_TASK_ID])
            
            elif task_status[STATE] == FINISHED and task_status[END_TIME] != None and task_status[START_TIME] != None:
                user_task.status[USER_TASK_START_TIME] = task_status[START_TIME]
                user_task.status[USER_TASK_END_TIME] = task_status[END_TIME]
                user_task.status[USER_TASK_STATUS] = FINISHED
                user_task.status[USER_TASK_DURATION] = task_status[END_TIME] - task_status[START_TIME]
                # print("reconcile: user_task finished", user_task.spec[USER_TASK_ID])
            return
                

    def bind_label_and_send_data(self, node_id, label,s3,bucket_name,object_key):
        

        @ray.remote(num_cpus=0.3)
        def bind_label():
            # print("s3:",s3,"  bucket name:",str(bucket_name),"  object_key:",object_key,"  working_dir:",label,"node_type:",os.getenv('LOCAL_NODE_TYPE'))

            def download_s3_folder(bucket_name, s3_folder='', local_dir=None,node_type=1):
                """
                Download the contents of a folder directory
                Args:
                    bucket_name: the name of the s3 bucket
                    s3_folder: the folder path in the s3 bucket
                    local_dir: a relative or absolute directory path in the local file system
                """

                bandwidth = {'0': None, '1': MAX_BANDWIDTH_HPC, '2': None}
                band_width=bandwidth[node_type]
                config=TransferConfig( max_bandwidth=band_width)
                s3=boto3.resource('s3')
                bucket = s3.Bucket(bucket_name)
                for obj in bucket.objects.filter(Prefix=s3_folder):
                    
                    if local_dir is None:
                        target = obj.key
                    elif local_dir[-1]=="/":
                        target=os.path.join(local_dir, os.path.relpath(obj.key, s3_folder))
                    else:
                        target=local_dir
                
                    if '/' in target:
                        if not os.path.exists(os.path.dirname(target)):
                            os.makedirs(os.path.dirname(target))
                        
                    if obj.key[-1] == '/':
                        continue
                    bucket.download_file(obj.key, target,Config=config)
	        #TODO: how to guarantee transfer data finished 
            #TODO: set home address
            # os.system("ls")
            # if os.path.exists(directory):
            #     ray.get_runtime_context().set_label({label: label})
            # else:
            #     os.system("rsync -a -P {} {}".format(label,node_ip+":"+label))
            
            if os.path.exists(label):
                # print("path exists")
                ray.get_runtime_context().set_label({label: label})
            else:
                # print("path not exists, s3 is ", s3)
                # node_ip=self.get_node_ip(data_id)
                if s3==True:
                    
                    node_type=os.getenv('LOCAL_NODE_TYPE')
                    # print("bucket name:",str(bucket_name),"  object_key:",object_key,"  working_dir:",label,"node_type:",node_type)
                    download_s3_folder(str(bucket_name),str(object_key),str(label),str(node_type))
                    ray.get_runtime_context().set_label({label: label})

                else:
                    # os.system(f"rsync -e 'ssh -o StrictHostKeyChecking=no' --mkpath -r -a -P {NODE_USER_NAME}@{DATA_IP}:{label} {label}")
                    # ray.get_runtime_context().set_label({label: label})

                    # make rsync fault tolerant
                    max_attempts = 3
                    attempts = 0
                    while attempts < max_attempts:
                        try:
                            result = subprocess.run(
                                ["rsync", "-e", "ssh -o StrictHostKeyChecking=no", "--mkpath", "-r", "-a", "-P",
                                f"{NODE_USER_NAME}@{DATA_IP}:{label}", label],
                                check=True
                            )
                            if result.returncode == 0:
                                ray.get_runtime_context().set_label({label: label})
                                break
                            else:
                                print(f"rsync failed with return code {result.returncode}")
                        except subprocess.CalledProcessError as e:
                            print(f"rsync command failed with error: {e}")
                        attempts += 1
                        if attempts < max_attempts:
                            print(f"Retrying rsync (attempt {attempts + 1} of {max_attempts})")
                            time.sleep(attempts)
                        else:
                            print(f"rsync failed after {max_attempts} attempts")
                    # write the final attempt to log in /tmp/rsync_log.txt, append to it
                    is_failed = attempts == max_attempts
                    status_log = "failed" if is_failed else "succeeded"
                    log_file_path = "/tmp/rsync_log.txt"
                    with open(log_file_path, "a") as f:
                        f.write(f"rsync {label} to {DATA_IP} {status_log} after {attempts} attempts\n")
            return FINISHED

        task_id = bind_label.options(
            scheduling_strategy=NodeLabelSchedulingStrategy(
                hard={"ray.io/node_id": In(node_id)}
            )
        ).remote()

        return task_id.hex()[:-8]

    def schedule(self, user_task):
        node_info = requests.get("http://localhost:8000/get/node-info").json()
        return self.get_best_node(node_info, user_task)

    
    def ray_schedule(self, user_task):
        node_info = requests.get("http://localhost:8000/get/node-info").json()
        if len(filtered_nodes) > 0:
            return random.choice(list(filtered_nodes.keys()))
        else:
            # No node available, requeue the task
            return None

    # This function is used in the case there are no available nodes
    # We estimate the finish time of the running/pending tasks on each node
    # and return the node with the earliest available time
    def get_best_node(self, node_info, user_task):
        # node_id_list = []
        # for node_id, node in node_info.items():
        #     node_id_list.append(node_id)
        # self.indicator = (self.indicator +1)%len(node_id_list)   
        # return node_id_list[self.indicator]

        earliest_time = float("inf")
        best_node = None
        current_time = int(time.time() * 1000)

        required_cpu = user_task.spec[CPU] if CPU in user_task.spec else 0
        required_gpu = user_task.spec[GPU] if GPU in user_task.spec else 0
        required_memory = user_task.spec[MEMORY] if MEMORY in user_task.spec else 0


        for node_id, node in node_info.items():
            available_cpu = node[AVAILABLE_CPU]
            available_gpu = node[AVAILABLE_GPU]
            available_memory = node[AVAILABLE_MEMORY]

            user_task_duration = user_task.spec[COMPLEXITY_SCORE] / node[SPEED]
            current_time = int(time.time() * 1000)
            user_task_estimated_finish_time =  current_time + user_task_duration

            if available_cpu >= required_cpu and available_gpu >= required_gpu and available_memory >= required_memory:
                if user_task_estimated_finish_time < earliest_time:
                    earliest_time = user_task_estimated_finish_time
                    best_node = node_id
                        
        if best_node is None:
            # estimate earliest available time and choose best node again
            for node_id, node in node_info.items():

                available_cpu = node[AVAILABLE_CPU]
                available_gpu = node[AVAILABLE_GPU]
                available_memory = node[AVAILABLE_MEMORY]

                user_task_duration = user_task.spec[COMPLEXITY_SCORE] / node[SPEED]
                current_time = int(time.time() * 1000)


                for _, task in node[RUNNING_OR_PENDING_TASKS].items():
                    task = Task(**task)
                    if task.status[USER_TASK_STATUS] == RUNNING:
                        start_time = task.status[USER_TASK_START_TIME]
                    else:
                        start_time = task.status[USER_TASK_ESTIMATED_START_TIME]

                    estimated_finish_time = start_time + task.spec[COMPLEXITY_SCORE] / node[SPEED] + user_task_duration

                    available_cpu += task.spec[CPU] if CPU in task.spec else 0
                    available_gpu += task.spec[GPU] if GPU in task.spec else 0
                    available_memory += task.spec[MEMORY] if MEMORY in task.spec else 0
                    
                    if available_cpu >= required_cpu and available_gpu >= required_gpu and available_memory >= required_memory:
                        if estimated_finish_time < earliest_time and estimated_finish_time > current_time:
                            earliest_time = estimated_finish_time
                            best_node = node_id
        
        if best_node == None or (node_info[best_node][PENDING_TASKS_COUNT] + 1) > MAX_PENDING_TASK:
            if best_node is None:
                print("[FIND BEST NODE] No available node found")
                # print all node's available resources for debugging
                for node_id, node in node_info.items():
                    print("[FIND BEST NODE] Check Node {}: CPU: {}".format(node_id, node[AVAILABLE_CPU]))
            else:
                print("[FIND BEST NODE] Node {} has too many pending tasks".format(best_node))
            return None

        user_task.status[USER_TASK_ESTIMATED_START_TIME] = earliest_time
        return best_node



    
    def filter_nodes(self, node_info, user_task):
        from ray.util.state import get_node
        filtered_nodes = {}
        for node_id, node in node_info.items():
            if CPU in user_task.spec and node[AVAILABLE_CPU] < user_task.spec[CPU]:
                continue
            if GPU in user_task.spec and node[AVAILABLE_GPU] < user_task.spec[GPU]:
                continue
            if MEMORY in user_task.spec and node[AVAILABLE_MEMORY] < user_task.spec[MEMORY]:
                continue
            filtered_nodes[node_id] = node
        return filtered_nodes
        

    def sort_by_speed(self, filtered_nodes):
        sorted_node_ids = sorted(filtered_nodes.keys(), key=lambda node_id: filtered_nodes[node_id][SPEED], reverse=True)
        return sorted_node_ids[0]

       
    def update_status(self, user_task):
        requests.post("http://localhost:8000/update-status", json=user_task.to_dict())

        

       
            