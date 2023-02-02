# __return_ray_put_start__
import ray

# Non-fault tolerant version:
@ray.remote
def A():
    x_ref = ray.put(1)
    return x_ref

x_ref = ray.get(A.remote())
# Object x outlives its owner task A.
try:
    # If owner of x (i.e. the worker process running task A) dies,
    # the application can no longer get value of x anymore.
    print(ray.get(x_ref))
except ray.exceptions.OwnerDiedError:
    pass


# Fault tolerant version:
@ray.remote
def B():
    return 1

# The owner of y is the driver
# so y is accessible and can be auto recovered
# during the entire lifetime of the driver.
y_ref = B.remote()
print(ray.get(y_ref))
# __return_ray_put_end__


# __return_child_task_start__
# Non-fault tolerant version:
@ray.remote
def A():
    return 1

@ray.remote
def B():
    x_ref = A.remote()
    return x_ref

# Object x outlives its owner task B.
x_ref = ray.get(B.remote())
try:
    # If owner of x (i.e. the worker process running task B) dies,
    # the application can no longer get value of x anymore.
    print(ray.get(x_ref))
except ray.exceptions.OwnerDiedError:
    pass

# Fault tolerant version:
@ray.remote
def C():
    x_ref = A.remote()
    # Here we create a new object y that has the same value as x
    y = ray.get(x_ref)
    return y

# The owner of y is the dirver
# so y is accessible and can be auto recovered
# during the entire lifetime of the driver.
y_ref = C.remote()
print(ray.get(y_ref))
# __return_child_task_end__


# __node_ip_resource_start__
@ray.remote
def D():
    return 1

# If the node with ip 127.0.0.3 fails while task D is running,
# Ray cannot retry the task on other nodes.
D.options(resources={"node:127.0.0.3":1}).remote()
# __node_ip_resource_end__
