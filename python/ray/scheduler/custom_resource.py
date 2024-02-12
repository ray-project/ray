from scheduler_constant import *

class Task():
    def __init__(self, spec={}, status={
            ASSIGN_NODE: None,
            BIND_TASK_ID: None,
            BIND_TASK_STATUS: None,
            BIND_TASK_DURATION: None,
            BIND_TASK_START_TIME: None,
            BIND_TASK_END_TIME: None,
            USER_TASK_STATUS: None,
            USER_TASK_DURATION: None,
            USER_TASK_START_TIME: None,
            USER_TASK_END_TIME: None,
            USER_TASK_ESTIMATED_START_TIME: None
        }):
        self.spec = spec
        self.status = status
    
    def to_dict(self):
        return {
            "spec": self.spec,
            "status": self.status
        }
    
    def __str__(self):
        return f'spec: {self.spec}, status: {self.status}'