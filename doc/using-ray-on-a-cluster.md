# Using Ray on a cluster

Deploying Ray on a cluster currently requires a bit of manual work.

## Deploying Ray on a cluster.

This section assumes that you have a cluster running and that the node in the
cluster can communicate with each other. It also assumes that Ray is installed
on each machine. To install Ray, follow the instructions for [installation on
Ubuntu](install-on-ubuntu.md).

### Starting Ray on each machine.

On the head node (just choose some node to be the head node), run the following.

```
./ray/scripts/start_ray.sh --head
```

This will print out the address of the Redis server that was started (and some
other address information).

Then on all of the other nodes, run the following. Make sure to replace
`<redis-address>` with the value printed by the command on the head node (it
should look something like `123.45.67.89:12345`).

```
./ray/scripts/start_ray.sh --redis-address <redis-address>
```

To specify the number of processes to start, use the flag `--num-workers`, as
follows:

```
./ray/scripts/start_ray.sh --num-workers <int>
```

Now we've started all of the Ray processes on each node Ray. This includes

- Some worker processes on each machine.
- An object store on each machine.
- A local scheduler on each machine.
- One Redis server (on the head node).
- One global scheduler (on the head node).

To run some commands, start up Python on one of the nodes in the cluster, and do
the following.

```python
import ray
ray.init(redis_address="<redis-address>")
```

Now you can define remote functions and execute tasks. For example:

```python
@ray.remote
def f(x):
  return x

ray.get([f.remote(f.remote(f.remote(0))) for _ in range(1000)])
```

### Stopping Ray
When you want to stop the Ray processes, run `./ray/scripts/stop_ray.sh`
on each node.

### Copying Application Files to Other Nodes (Experimental)

If you're running an application that imports Python files that are present
locally but not on the other machines in the cluster, you may first want to copy
those files to the other machines. One way to do that is through Ray (this is
experimental). Suppose you're directory structure looks

```
application_files/
    __init__.py
    example.py
```

And suppose `example.py` defines the following functions.

```python
import ray

def example_helper(x):
  return x

@ray.remote
def example_function(x):
  return example_helper(x)
```

If you simply run

```python
from application_files import example
```

An error message will be printed like the following. This indicates that one of
the workers was unable to register the remote function.

```
Traceback (most recent call last):
  File "/home/ubuntu/ray/lib/python/ray/worker.py", line 813, in fetch_and_register_remote_function
    function = pickling.loads(serialized_function)
ImportError: No module named 'application_files'
```

To make this work, you need to copy your application files to all of the nodes.
The following command will do that through Ray, and will add the files to Python
path of each worker. This functionality is experimental. You may be able to do
something like the following.

```python
import ray

ray.init(redis_address="<redis-address>")

ray.experimental.copy_directory("application_files/")

# Now the import should work.
from application_files import example
```

Now you should be able to run the following command.

```python
ray.get([example.example_function.remote(0) for _ in range(1000)])
```
