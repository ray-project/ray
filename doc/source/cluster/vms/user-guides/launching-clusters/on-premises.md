(on-prem)=

# Launching an On-Premise Cluster

This document describes how to set up an on-premise Ray cluster, i.e., to run Ray on bare metal machines, or in a private cloud. We provide two ways to start an on-premise cluster.

* If you know all of the nodes in advance and have SSH access to them, use the Ray [cluster launcher](manual-cluster-launcher).
* Otherwise, you can [manually set up](manual-setup-cluster) the Ray cluster by installing the Ray package and starting the Ray processes on each node.

(manual-cluster-launcher)=

## Using Ray cluster launcher

The Ray cluster launcher is part of the `ray` command line tool. It allows you to start, stop and attach to a running ray cluster using commands such as  `ray up`, `ray down` and `ray attach`. You can use pip to install it, or follow [install ray](installation) for more detailed instructions.

```bash
# install ray
pip install "ray[default]"
```

### Start Ray with the Ray cluster launcher

The provided [example-full.yaml](https://github.com/ray-project/ray/tree/eacc763c84d47c9c5b86b26a32fd62c685be84e6/python/ray/autoscaler/local/example-full.yaml) cluster config file will create a Ray cluster given a list of nodes.

Note that you'll need to fill in your [head_ip](https://github.com/ray-project/ray/blob/eacc763c84d47c9c5b86b26a32fd62c685be84e6/python/ray/autoscaler/local/example-full.yaml#L20), a list of [worker_ips](https://github.com/ray-project/ray/blob/eacc763c84d47c9c5b86b26a32fd62c685be84e6/python/ray/autoscaler/local/example-full.yaml#L26), and the [ssh_user](https://github.com/ray-project/ray/blob/eacc763c84d47c9c5b86b26a32fd62c685be84e6/python/ray/autoscaler/local/example-full.yaml#L34) field in those templates



Test that it works by running the following commands from your local machine:

```bash
# Download the example-full.yaml
wget https://raw.githubusercontent.com/ray-project/ray/master/python/ray/autoscaler/local/example-full.yaml

# Update the example-full.yaml to update head_ip, worker_ips, and ssh_user.
# vi example-full.yaml

# Create or update the cluster. When the command finishes, it will print
# out the command that can be used to SSH into the cluster head node.
ray up example-full.yaml

# Get a remote screen on the head node.
ray attach example-full.yaml

# Check the cluster status and make sure you eventually see all nodes join.
ray status example-full.yaml

# ...Try running a simple Ray program...
python -c "import ray; ray.init(); print(ray.available_resources())"

# Tear down the cluster.
ray down example-full.yaml
```

Congratulations, you have started a local Ray cluster!

Check out [job submission](jobs-quickstart) next for instructions on running Ray applications on the cluster from your laptop.

### Troubleshooting: Head node launches but worker nodes do not

This can happen if the head node cannot SSH into the worker nodes and/or the head and worker nodes cannot connect to each other's ports.

In this case, after running ``ray up example-full.yaml`` attach to the head node and check that you can SSH into the worker node using the following commands:

```bash
# Get a remote screen on the head node.
ray attach example-full.yaml

# Check that you can SSH from the head node to the worker node.
ssh -i ~/ray_bootstrap_key.pem <worker node IP>
```

To check that ports are accessible, use a tool like ``telnet <ip> <port>``. The ports to check are:

- The head node port (6379 by default)
- The raylet port on all nodes, including the head node. Ray chooses a random port by default for the raylet, but you can override this by passing `--node-manager-port <port>` to `ray start`.

If you are using cloud VMs, a common pitfall that can cause ports to be unreachable is the security group configuration.
In this case, check if you can use Ray's cloud [cluster launcher](cloud-vm-index) to create clusters instead; otherwise, check your security group configuration.

(manual-setup-cluster)=

## Manually Set up a Ray Cluster
This section assumes that you have a list of machines and that the nodes in the cluster share the same network. It also assumes that Ray is installed on each machine. You can use pip to install the ray command line tool with cluster launcher support. Follow the [Ray installation instructions](installation) for more details.

```bash
# install ray
pip install -U "ray[default]"
```

### Start the Head Node
Choose any node to be the head node and run the following. If the `--port` argument is omitted, Ray will first choose port 6379, and then fall back to a random port if in 6379 is in use.

```bash
ray start --head --port=6379
```

The command will print out the Ray cluster address, which can be passed to `ray start` on other machines to start the worker nodes (see below). If you receive a ConnectionError, check your firewall settings and network configuration.

### Start Worker Nodes
Then on each of the other nodes, run the following command to connect to the head node you just created.

```bash
ray start --address=<head-node-address:port>
```
Make sure to replace `head-node-address:port` with the value printed by the command on the head node (it should look something like 123.45.67.89:6379).

Note that if your compute nodes are on their own subnetwork with Network Address Translation, the address printed by the head node will not work if connecting from a machine outside that subnetwork. You will need to use a head node address reachable from the remote machine. If the head node has a domain address like compute04.berkeley.edu, you can simply use that in place of an IP address and rely on DNS.

Ray auto-detects the resources (e.g., CPU) available on each node, but you can also manually override this by passing custom resources to the `ray start` command. For example, if you wish to specify that a machine has 10 CPUs and 1 GPU available for use by Ray, you can do this with the flags `--num-cpus=10` and `--num-gpus=1`.
See the [Configuration page](configuring-ray) for more information.

### Troubleshooting

If you see `Unable to connect to GCS at ...`, this means the head node is inaccessible at the given `--address`.
Some possible causes include:

- the head node is not actually running;
- a different version of Ray is running at the specified address;
- the specified address is wrong;
- or there are firewall settings preventing access.

If the connection fails, to check whether each port can be reached from a node, you can use a tool such as nmap or nc.

```bash
$ nmap -sV --reason -p $PORT $HEAD_ADDRESS
Nmap scan report for compute04.berkeley.edu (123.456.78.910)
Host is up, received echo-reply ttl 60 (0.00087s latency).
rDNS record for 123.456.78.910: compute04.berkeley.edu
PORT     STATE SERVICE REASON         VERSION
6379/tcp open  redis?  syn-ack
Service detection performed. Please report any incorrect results at https://nmap.org/submit/ .
$ nc -vv -z $HEAD_ADDRESS $PORT
Connection to compute04.berkeley.edu 6379 port [tcp/*] succeeded!
```

If the node cannot access that port at that IP address, you might see

```bash
$ nmap -sV --reason -p $PORT $HEAD_ADDRESS
Nmap scan report for compute04.berkeley.edu (123.456.78.910)
Host is up (0.0011s latency).
rDNS record for 123.456.78.910: compute04.berkeley.edu
PORT     STATE  SERVICE REASON       VERSION
6379/tcp closed redis   reset ttl 60
Service detection performed. Please report any incorrect results at https://nmap.org/submit/ .
$ nc -vv -z $HEAD_ADDRESS $PORT
nc: connect to compute04.berkeley.edu port 6379 (tcp) failed: Connection refused
```
