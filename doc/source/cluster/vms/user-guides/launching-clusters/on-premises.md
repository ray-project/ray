---
myst:
  html_meta:
    description: "Set up a Ray cluster on bare metal or a private cloud, either manually with ray start or through the cluster launcher."
---

(on-prem)=

# Launching an On-Premise Cluster

This document describes how to set up an on-premise Ray cluster, i.e., to run Ray on bare metal machines, or in a private cloud. We provide two ways to start an on-premise cluster.

* You can [manually set up](manual-setup-cluster) the Ray cluster by installing the Ray package and starting the Ray processes on each node.
* Alternatively, if you know all the nodes in advance and have SSH access to them, you should start the Ray cluster using the [cluster-launcher](manual-cluster-launcher).

(manual-setup-cluster)=

## Manually Set up a Ray Cluster
This section assumes that you have a list of machines and that the nodes in the cluster share the same network. It also assumes that Ray is installed on each machine. You can use pip to install the ray command line tool with cluster launcher support. Follow the [Ray installation instructions](installation) for more details.

```bash
# install ray
pip install -U "ray[default]"
```

### Open the ports Ray uses

Ray needs bidirectional network access between every pair of nodes, not only from the worker nodes to the head node. Each node listens on several ports, and the head node listens on a few more, such as 6379 for the GCS server and 8265 for the dashboard. Several of the per-node ports default to random values, so a firewall that allows only a fixed list of ports blocks them. Either allow all traffic between the cluster's nodes, or pin each port with its `ray start` option and open those ports. For the full list of ports and options, see {ref}`ray-ports`.

### Start the Head Node
Choose any node to be the head node. Starting in Ray 2.61, {ref}`token authentication <token-auth>` is on by default, and `ray start --head` fails if no token exists. Create the token once on the head node, then start Ray. If you omit the `--port` argument, Ray first chooses port 6379, and then falls back to a random port if 6379 is in use.

```bash
ray get-auth-token --generate
ray start --head --port=6379
```

The command will print out the Ray cluster address, which can be passed to `ray start` on other machines to start the worker nodes (see below). If you receive a ConnectionError, check your firewall settings and network configuration.

### Start Worker Nodes
Copy the head node's token file, `~/.ray/auth_token`, to the same path on each of the other nodes. Don't pass the token on a command line. A worker node without the token fails to join with an error that says to copy it from the head node.

Then on each of the other nodes, run the following command to connect to the head node you just created.

```bash
ray start --address=<head-node-address:port>
```
Make sure to replace `head-node-address:port` with the value printed by the command on the head node (it should look something like 123.45.67.89:6379).

To run the cluster without authentication, set `RAY_AUTH_MODE=disabled` on every node and every client of the cluster instead of copying the token. See {ref}`token-auth` for details.

Note that if your compute nodes are on their own subnetwork with Network Address Translation, the address printed by the head node will not work if connecting from a machine outside that subnetwork. You will need to use a head node address reachable from the remote machine. If the head node has a domain address like compute04.berkeley.edu, you can simply use that in place of an IP address and rely on DNS.

Ray auto-detects the resources (e.g., CPU) available on each node, but you can also manually override this by passing custom resources to the `ray start` command. For example, if you wish to specify that a machine has 10 CPUs and 1 GPU available for use by Ray, you can do this with the flags `--num-cpus=10` and `--num-gpus=1`. See the [Configuration page](configuring-ray) for more information.

### Troubleshooting

If you see `Unable to connect to GCS at ...`, this means the head node is inaccessible at the given `--address`. Some possible causes include:

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
(manual-cluster-launcher)=

## Using Ray cluster launcher

The Ray cluster launcher is part of the `ray` command line tool. It allows you to start, stop and attach to a running ray cluster using commands such as  `ray up`, `ray down` and `ray attach`. You can use pip to install it, or follow [install ray](installation) for more detailed instructions.

```bash
# install ray
pip install "ray[default]"
```

### Start Ray with the Ray cluster launcher

The provided [example-full.yaml](https://github.com/ray-project/ray/blob/eacc763c84d47c9c5b86b26a32fd62c685be84e6/python/ray/autoscaler/local/example-full.yaml) cluster config file will create a Ray cluster given a list of nodes.

Note that you'll need to fill in your [head_ip](https://github.com/ray-project/ray/blob/eacc763c84d47c9c5b86b26a32fd62c685be84e6/python/ray/autoscaler/local/example-full.yaml#L20), a list of [worker_ips](https://github.com/ray-project/ray/blob/eacc763c84d47c9c5b86b26a32fd62c685be84e6/python/ray/autoscaler/local/example-full.yaml#L26), and the [ssh_user](https://github.com/ray-project/ray/blob/eacc763c84d47c9c5b86b26a32fd62c685be84e6/python/ray/autoscaler/local/example-full.yaml#L34) field in those templates

`ray up` sets up token authentication for the cluster with no extra steps. See {ref}`vm-cluster-token-auth`.



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
# Try running a Ray program.

# Tear down the cluster.
ray down example-full.yaml
```

Congrats, you have started a local Ray cluster!
