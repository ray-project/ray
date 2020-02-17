import ray

@ray.remote(num_cpus=5)
class Bar:
    def __init__(self):
        self.x = 2

    def get(self):
        return self.x

ray.init(num_cpus=8, hosted_dashboard_addr="localhost:8080")
bar1 = Bar.remote()
bar2 = Bar.remote()
print(ray.get(bar1.get.remote())) 