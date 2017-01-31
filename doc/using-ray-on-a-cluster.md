# Using Ray on a cluster

Deploying Ray on a cluster currently requires a bit of manual work.

## Booting up a cluster on EC2

TODO

## Deploying Ray on a cluster.

This section assumes that you have a cluster of machines running, that these nodes
have network connectivity to one another. It also assumes that Ray is installed
on each machine. To install Ray, follow the instructions for [installation on
Ubuntu](install-on-ubuntu.md).

Assumptions

* All of the following commands are run from a machine designated as
the _head node_.
* The head node will run Redis and the global scheduler.
* The head node is the launching point for driver programs and for administrative tasks.
* All nodes are accessible via ssh keys

### Connect to the head node

```
ssh -A ubuntu@raycluster
```

The `-A` parameter for `ssh` enables agent forwarding, which will allow your session to connect to other nodes in the cluster without further authentication.

### Build a list of node IP addresses

Populate a file `workers.txt` with one IP address on each line.
Do not include the head node ip address in this file.

### Confirm that you can ssh to all nodes
```
for host in $(cat workers.txt); do
	ssh $host uptime
done
```

You may be prompted to verify the host keys during this process.

### Starting Ray

#### Starting Ray on the head node

On the head node (just choose some node to be the head node), run the following,
replacing `<redis-port>` with a port of your choice, e.g., `6379`.
Also, replace `<num-workers>` with the number of workers that you wish to start.

TODO: do we want to run any workers on the head node? Maybe this should be 0 to ensure responsiveness of the global scheduler and Redis.

```
./ray/scripts/start_ray.sh --head --num-workers=<num-workers> --redis-port <redis-port>
```

#### Start Ray on the worker nodes

Edit a file `start_worker.sh` to include something like the following:

```
export PATH=/home/ubuntu/anaconda2/bin/:$PATH
ray/scripts/start_ray.sh --num-workers=<num-workers> --redis-address=<head-node-ip>:<redis-port>
```

This is the script that when run on the worker nodes will start up Ray.
You will need to replace `<head-node-ip>` with the IP address that worker nodes will
use to connect to the head node (most likely a private network address).
In this example we also export the path to the Python installation since our remote
commands will not be executing in a login shell.

Now use `parallel-ssh` to start up Ray on each worker node.

```
parallel-ssh -h workers.txt -P -I < start_worker.sh
```

Note that on some distributions the `parallel-ssh` command may be called `pssh`.

#### Verification

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

#### Stop Ray on worker nodes

```
parallel-ssh -h workers.txt -P ray/scripts/stop_ray.sh
```

#### Stop Ray on the head node

```
ray/scripts/stop_ray.sh
```


## Copying Application Files to Other Nodes (Experimental)

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
