import ray

class DAGRunner:
    def __init__(self, dag):
        # TODO: (jiaodong) Make this class take JSON serialized dag
        self.dag = dag

    def __call__(self, request):
        return ray.get(self.dag(request))