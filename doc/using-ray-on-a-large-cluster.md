# Using Ray on a large cluster

Deploying Ray on a cluster currently requires a bit of manual work. The
instructions here illustrate how to use parallel ssh commands to simplify the
process of running commands and scripts on many machines simultaneously.

## Booting up a cluster on EC2

* Create an EC2 instance running Ray following instructions for [installation on
Ubuntu](install-on-ubuntu.md).
    * Add any packages that you may need for running your application.
    * Install the pssh package: `sudo apt-get install pssh`
* [Create an AMI Image](http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/creating-an-ami-ebs.html)
of your installation.
* Use the EC2 console to launch additional instances using the AMI created.

## Deploying Ray on a cluster.

This section assumes that you have a cluster of machines running and that these
nodes have network connectivity to one another. It also assumes that Ray is
installed on each machine.

Additional assumptions:

* All of the following commands are run from a machine designated as
  the _head node_.
* The head node will run Redis and the global scheduler.
* The head node is the launching point for driver programs and for
  administrative tasks.
* The head node has ssh access to all other nodes.
* All nodes are accessible via ssh keys
* Ray is checked out on each node at the location `$HOME/ray`.

**Note:** The commands below will probably need to be customized for your specific
setup.

### Connect to the head node

In order to initiate ssh commands from the cluster head node we suggest enabling
ssh agent forwarding. This will allow the session that you initiate with the
head node to connect to other nodes in the cluster to run scripts on them. You
can enable ssh forwarding by running the following command (replacing
`<ssh-key>` with the path to the private key that you would use when logging in
to the nodes in the cluster).

```
ssh-add <ssh-key>
```

Now log in to the head node with the following command, where
`<head-node-public-ip>` is the public IP address of the head node (just choose
one of the nodes to be the head node).

```
ssh -A ubuntu@<head-node-public-ip>
```

### Build a list of node IP addresses

Populate a file `workers.txt` with one IP address on each line. Do not include
the head node IP address in this file. These IP addresses should typically be
private network IP addresses, but any IP addresses which the head node can use
to ssh to worker nodes will work here.

### Confirm that you can ssh to all nodes

```
for host in $(cat workers.txt); do
	ssh $host uptime
done
```

You may be prompted to verify the host keys during this process.

### Starting Ray

#### Starting Ray on the head node

On the head node (just choose some node to be the head node), run the following:

```
./ray/scripts/start_ray.sh --head --num-workers=<num-workers> --redis-port <redis-port>
```

Replace `<redis-port>` with a port of your choice, e.g., `6379`. Also, replace
`<num-workers>` with the number of workers that you wish to start.


#### Start Ray on the worker nodes

Create a file `start_worker.sh` that contains something like the following:

```
export PATH=/home/ubuntu/anaconda2/bin/:$PATH
ray/scripts/start_ray.sh --num-workers=<num-workers> --redis-address=<head-node-ip>:<redis-port>
```

This script, when run on the worker nodes, will start up Ray. You will need to
replace `<head-node-ip>` with the IP address that worker nodes will use to
connect to the head node (most likely a private network address). In this
example we also export the path to the Python installation since our remote
commands will not be executing in a login shell.

**Warning:** You may need to manually export the correct path to Python (you
will need to change the first line of `start_worker.sh` to find the version of
Python that Ray was built against). This is necessary because the `PATH`
environment variable used by `parallel-ssh` can differ from the `PATH`
environment variable that gets set when you `ssh` to the machine.

**Warning:** If the `parallel-ssh` command below appears to hang, the
`head-node-ip` may need to be a private IP address instead of a public IP
address (e.g., if you are using EC2).

Now use `parallel-ssh` to start up Ray on each worker node.

```
parallel-ssh -h workers.txt -P -I < start_worker.sh
```

Note that on some distributions the `parallel-ssh` command may be called `pssh`.

#### Verification

Now you have started all of the Ray processes on each node. These include:

- Some worker processes on each machine.
- An object store on each machine.
- A local scheduler on each machine.
- One Redis server (on the head node).
- One global scheduler (on the head node).

To confirm that the Ray cluster setup is working, start up Python on one of the
nodes in the cluster and enter the following commands to connect to the Ray
cluster.

```python
import ray
ray.init(redis_address="<redis-address>")
```

Here `<redis-address>` should have the form `<head-node-ip>:<redis-port>`.

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

This command will execute the `stop_ray.sh` script on each of the worker nodes.

#### Stop Ray on the head node

```
ray/scripts/stop_ray.sh
```


## Sync Application Files to other nodes

If you are running an application that reads input files or uses python libraries then you may find it useful to copy a directory on the head to the worker nodes.


You can do this using the `parallel-rsync` command:

```
parallel-rsync -h workers.txt -r <workload-dir> /home/ubuntu/<workload-dir>
```

where `<workload-dir>` is the directory you want to synchronize.
Note that the destination argument for this command must represent an absolute path on the worker node.
