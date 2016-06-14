# Ray

Ray is an experimental distributed execution framework with a Python-like
programming model. It is under development and not ready for general use.

## Example Code

### Loading ImageNet
TODO: fill this out.

## Design Decisions

For a description of our design decisions, see

- [Reference Counting](doc/reference-counting.md)
- [Aliasing](doc/aliasing.md)
- [Scheduler](doc/scheduler.md)

## Setup

1. sudo apt-get update
2. sudo apt-get install git
3. git clone https://github.com/amplab/ray.git
4. cd ray
5. ./setup.sh

## Installing Ray on a cluster

These instructions work on EC2, but they may require some modifications to run
on your own cluster. In particular, on EC2, running `sudo` does not require a
password, and we currently don't handle the case where a password is needed.

1. Create a file `nodes.txt` of the IP addresses of the nodes in the cluster.
For example

        52.50.28.103
        52.51.210.207
2. Make sure that the nodes can all communicate with one another. On EC2, this
can be done by creating a new security group and adding the inbound rule "all
traffic" and adding the outbound rule "all traffic". Then add all of the nodes
in your cluster to that security group.

3. Run something like
    ```
    python scripts/cluster.py --nodes nodes.txt \
                              --key-file key.pem \
                              --username ubuntu \
                              --installation-directory /home/ubuntu/
    ```
where you replace `nodes.txt`, `key.pem`, `ubuntu`, and `/home/ubuntu/` by the
appropriate values. This assumes that you can connect to each IP address in
`nodes.txt` with the command
    ```
    ssh -i key.pem ubuntu@<ip-address>
    ```
4. The previous command should open a Python interpreter. To install Ray on the
cluster, run `install_ray(node_addresses)` in the interpreter. The interpreter
should block until the installation has completed.
5. To check that the installation succeeded, you can ssh to each node, cd into
the directory `ray/test/`, and run the tests (e.g., `python runtest.py`).
6. Now that Ray has been installed, you can start the cluster (the scheduler,
object stores, and workers) with the command `start_ray(node_addresses,
"/home/ubuntu/ray/test/test_worker.py")`, where the second argument is the path
on each node in the cluster to the worker code that you would like to use.
