# Launching On-Premises Cluster

This docs describes how to set up an on-premises Ray cluster, i.e., to run Ray on bare metal machines, or in a private cloud. We provide two ways to start an on-premises cluster.

* If you know all the nodes in advance and have ssh access to them, you should start the ray cluster using **cluster-launcher**.
* Alternatively, you can **manually set up** the Ray cluster by installing the Ray package and starting the Ray processes on each node. 

## Using Ray Cluster Launcher 
### Install Ray Cluster Launcher
The Ray cluster launcher is part of the `ray` command line tool. It allows you to start, stop and attach to a running ray cluster using commands such as  `ray up`, `ray down` and `ray attach`. You can use pip to install it, or follow [install ray](https://docs.ray.io/en/latest/ray-overview/installation.html) for more detailed instructions.

```
# install ray cluster launcher, which is part of ray command line tool.
pip install ray
```

### Start Ray with Ray Cluster Launcher


The provided [example-full.yaml](https://github.com/ray-project/ray/tree/eacc763c84d47c9c5b86b26a32fd62c685be84e6/python/ray/autoscaler/azure/example-full.yaml) cluster config file will create a small cluster with a Standard DS2v3 node (on-demand) configured to autoscale up to two Standard DS2v3 worker nodes ([spot-instances](https://docs.microsoft.com/en-us/azure/virtual-machines/windows/spot-vms)).

Note that you'll need to fill in your [resource group](https://github.com/ray-project/ray/blob/eacc763c84d47c9c5b86b26a32fd62c685be84e6/python/ray/autoscaler/azure/example-full.yaml#L42) and [location](https://github.com/ray-project/ray/blob/eacc763c84d47c9c5b86b26a32fd62c685be84e6/python/ray/autoscaler/azure/example-full.yaml#L41) in those templates. You also need set subscription to use from the command line `az account set -s <subscription_id>` or by filling the [subscription_id](https://github.com/ray-project/ray/blob/eacc763c84d47c9c5b86b26a32fd62c685be84e6/python/ray/autoscaler/azure/example-full.yaml#L44) in the cluster config.



Test that it works by running the following commands from your local machine:

```
# Download the example-full.yaml
wget https://raw.githubusercontent.com/ray-project/ray/master/python/ray/autoscaler/azure/example-full.yaml

# Update the example-full.yaml to update resource group, location, and subscription_id.
# vi example-full.yaml

# Create or update the cluster. When the command finishes, it will print
# out the command that can be used to SSH into the cluster head node.
ray up example-full.yaml

# Get a remote screen on the head node.
ray attach example-full.yaml
# Try running a Ray program.

# Tear down the cluster.
ray down example-full.yaml
```

Congrats, you have started a Ray cluster on Azure!

## Manually Set up a Ray Cluster 

The most preferable way to run a Ray cluster is via the Ray Cluster Launcher. However, it is also possible to start a Ray cluster by hand.

This section assumes that you have a list of machines and that the nodes in the cluster can communicate with each other. It also assumes that Ray is installed on each machine.  You can use pip to install it, or follow [install ray](https://docs.ray.io/en/latest/ray-overview/installation.html) for more detailed instructions.

```
# install ray cluster launcher, which is part of ray command line tool.
pip install ray
```


### Starting Ray on each machine
On the head node (just choose one node to be the head node), run the following. If the --port argument is omitted, Ray will choose port 6379, falling back to a random port.

```
$ ray start --head --port=6379
...
Next steps
  To connect to this Ray runtime from another node, run
    ray start --address='<ip address>:6379'
```

If connection fails, check your firewall settings and network configuration.
The command will print out the address of the Ray GCS server that was started (the local node IP address plus the port number you specified).