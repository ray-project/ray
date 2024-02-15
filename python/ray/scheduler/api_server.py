import requests
from fastapi import FastAPI
from ray import serve
from custom_resource import Task
import queue
from fastapi import Body
import logging
from scheduler_constant import *
import ray

app = FastAPI()

@serve.deployment(route_prefix="/")
@serve.ingress(app)
class Apiserver:
    def __init__(self):
        self.unfinished_user_tasks = queue.Queue()
        self.node_info = {}
        
        nodes = ray.nodes()
        for node in nodes:
            if node[NODE_ID] not in self.node_info:
                self.node_info[node[NODE_ID]] = {}
                self.node_info[node[NODE_ID]][NODE_CPU] = node[RESOURCE][NODE_CPU] if NODE_CPU in node[RESOURCE] else 0
                self.node_info[node[NODE_ID]][NODE_GPU] = node[RESOURCE][NODE_GPU] if NODE_GPU in node[RESOURCE] else 0
                self.node_info[node[NODE_ID]][NODE_MEMORY] = node[RESOURCE][NODE_MEMORY] if NODE_MEMORY in node[RESOURCE] else 0

                self.node_info[node[NODE_ID]][AVAILABLE_CPU] = self.node_info[node[NODE_ID]][NODE_CPU]
                self.node_info[node[NODE_ID]][AVAILABLE_GPU] = self.node_info[node[NODE_ID]][NODE_GPU]
                self.node_info[node[NODE_ID]][AVAILABLE_MEMORY] = self.node_info[node[NODE_ID]][NODE_MEMORY]

                self.node_info[node[NODE_ID]][TOTAL_DURATION] = 0
                self.node_info[node[NODE_ID]][TOTAL_COMPLEXITY_SCORE] = 0
                self.node_info[node[NODE_ID]][SPEED] = MAX_COMPLEXITY_SCORE
                self.node_info[node[NODE_ID]][RUNNING_OR_PENDING_TASKS] = {}

                self.node_info[node[NODE_ID]][PENDING_TASK_COUNT] = 0
                


    @app.post("/apply")
    def apply(self, spec: dict = Body(...)):
        user_task = Task(spec=spec)
        self.unfinished_user_tasks.put(user_task)
        print("apiserver: apply a new user_task", user_task.spec[USER_TASK_ID])

    @app.get("/get")
    def get(self):
        if self.unfinished_user_tasks.empty():
            return Task().to_dict()
        else:
            return self.unfinished_user_tasks.get().to_dict()

    @app.post("/update-status")
    def update_status(self, user_task: dict = Body(...)):
        user_task = Task(**user_task)

        if user_task.status[USER_TASK_STATUS] == PENDING:
            self._increase_pending_task_count(user_task)
            self._add_or_update_node_running_or_pending_task(user_task)
            self.unfinished_user_tasks.put(user_task)

        elif user_task.status[USER_TASK_STATUS] == RUNNING:
            self._decrease_pending_task_count(user_task)
            self._add_or_update_node_running_or_pending_task(user_task)
            self.unfinished_user_tasks.put(user_task)

        elif user_task.status[USER_TASK_STATUS] == FINISHED:
            self._update_node_speed_info(user_task)
            self._remove_node_running_task(user_task)

        else:
            print("apiserver: invalid user_task status", user_task.spec[USER_TASK_ID], user_task.status[USER_TASK_STATUS])

    @app.get("/get/node-info")
    def get(self):
        return self._get_node_info()


    def _get_node_info(self):
        # Uncomment this block if we enable the autoscaler
        # nodes = ray.nodes()
        # for node in nodes:
        #     if node[NODE_ID] not in self.node_info:
        #         self.node_info[node[NODE_ID]] = {}
        #         self.node_info[node[NODE_ID]][NODE_CPU] = node[RESOURCE][NODE_CPU]
        #         self.node_info[node[NODE_ID]][NODE_MEMORY] = node[RESOURCE][NODE_MEMORY]
        #         self.node_info[node[NODE_ID]][TOTAL_DURATION] = 0
        #         self.node_info[node[NODE_ID]][TOTAL_COMPLEXITY_SCORE] = 0
        #         self.node_info[node[NODE_ID]][SPEED] = 0
        return self.node_info

    def _increase_pending_task_count(self, user_task):
        assign_node = user_task.status[ASSIGN_NODE]
        if assign_node != None:
            self.node_info[assign_node][PENDING_TASK_COUNT] += 1

    def _decrease_pending_task_count(self, user_task):
        assign_node = user_task.status[ASSIGN_NODE]
        if assign_node != None:
            self.node_info[assign_node][PENDING_TASK_COUNT] -= 1

    def _update_node_speed_info(self, user_task):
        assign_node = user_task.status[ASSIGN_NODE]
        complexity_score = user_task.spec[COMPLEXITY_SCORE]
        duration = user_task.status[USER_TASK_DURATION] + user_task.status[BIND_TASK_DURATION]

        self.node_info[assign_node][TOTAL_DURATION] += duration
        self.node_info[assign_node][TOTAL_COMPLEXITY_SCORE] += complexity_score
        self.node_info[assign_node][SPEED] = self.node_info[assign_node][TOTAL_COMPLEXITY_SCORE] / self.node_info[assign_node][TOTAL_DURATION]

        # print all node speed
        for node_id, node in self.node_info.items():
            print("apiserver: node speed", node_id, self.node_info[node_id][SPEED])

    def _remove_node_running_task(self, user_task):
        assign_node = user_task.status[ASSIGN_NODE]
        del self.node_info[assign_node][RUNNING_OR_PENDING_TASKS][user_task.spec[USER_TASK_ID]]

        self.node_info[assign_node][AVAILABLE_CPU] += user_task.spec[CPU] if CPU in user_task.spec else 0
        self.node_info[assign_node][AVAILABLE_GPU] += user_task.spec[GPU] if GPU in user_task.spec else 0
        self.node_info[assign_node][AVAILABLE_MEMORY] += user_task.spec[MEMORY] if MEMORY in user_task.spec else 0

    def _add_or_update_node_running_or_pending_task(self, user_task):
        assign_node = user_task.status[ASSIGN_NODE]
        if assign_node == None:
            return
        if user_task.spec[USER_TASK_ID] not in self.node_info[assign_node][RUNNING_OR_PENDING_TASKS]:
            self.node_info[assign_node][AVAILABLE_CPU] -= user_task.spec[CPU] if CPU in user_task.spec else 0
            self.node_info[assign_node][AVAILABLE_GPU] -= user_task.spec[GPU] if GPU in user_task.spec else 0
            self.node_info[assign_node][AVAILABLE_MEMORY] -= user_task.spec[MEMORY] if MEMORY in user_task.spec else 0

        self.node_info[assign_node][RUNNING_OR_PENDING_TASKS][user_task.spec[USER_TASK_ID]] = user_task



        
        

