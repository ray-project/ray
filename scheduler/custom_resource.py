

class Resource():
    def __init__(self, spec={}, status={
            "assign_node": None,
            "bind_label": None,
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