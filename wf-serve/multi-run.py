import ray
from ray import workflow
ray.init(address='auto')
workflow.init()

@workflow.step
def s():
    return 10


print(s.step().run())
