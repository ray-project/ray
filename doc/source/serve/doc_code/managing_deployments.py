from ray import serve
import time
import os


# __updating_a_deployment_start__
@serve.deployment(name="my_deployment", num_replicas=1)
class SimpleDeployment:
    pass


# Creates one initial replica.
serve.run(SimpleDeployment.bind())


# Re-deploys, creating an additional replica.
# This could be the SAME Python script, modified and re-run.
@serve.deployment(name="my_deployment", num_replicas=2)
class SimpleDeployment:
    pass


serve.run(SimpleDeployment.bind())

# You can also use Deployment.options() to change options without redefining
# the class. This is useful for programmatically updating deployments.
serve.run(SimpleDeployment.options(num_replicas=2).bind())
# __updating_a_deployment_end__


# __scaling_out_start__
# Create with a single replica.
@serve.deployment(num_replicas=1)
def func(*args):
    pass


serve.run(func.bind())

# Scale up to 3 replicas.
serve.run(func.options(num_replicas=3).bind())

# Scale back down to 1 replica.
serve.run(func.options(num_replicas=1).bind())
# __scaling_out_end__


# __autoscaling_start__
@serve.deployment(
    autoscaling_config={
        "min_replicas": 1,
        "initial_replicas": 2,
        "max_replicas": 5,
        "target_num_ongoing_requests_per_replica": 10,
    }
)
def func(_):
    time.sleep(1)
    return ""


serve.run(
    func.bind()
)  # The func deployment will now autoscale based on requests demand.
# __autoscaling_end__


# __configure_parallism_start__
@serve.deployment
class MyDeployment:
    def __init__(self, parallelism: str):
        os.environ["OMP_NUM_THREADS"] = parallelism
        # Download model weights, initialize model, etc.

    def __call__(self):
        pass


serve.run(MyDeployment.bind("12"))
# __configure_parallism_end__
