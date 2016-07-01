# Ray

[![Build Status](https://travis-ci.org/amplab/ray.svg?branch=master)](https://travis-ci.org/amplab/ray)

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

### Linux, Mac, and other Unix-based systems

After running these instruction, add the line `source "$RAY_ROOT/setup-env.sh"` in your `~/.bashrc` file manually, where "$RAY_ROOT" is the path of the directory containing `setup-env.sh`.

1. sudo apt-get update
2. sudo apt-get install git
3. git clone https://github.com/amplab/ray.git
4. cd ray
5. ./setup.sh
6. ./build.sh
7. source setup-env.sh

### Windows

**Note:** A batch file is provided that clones any missing third-party libraries and applies patches to them.
Do not attempt to open the solution before the batch file applies the patches; otherwise, if the projects have been modified, the patches may be rejected, and you may be forced to revert your changes before re-running the batch file.

1. Install Microsoft Visual Studio 2015
2. Install Git
3. git clone https://github.com/amplab/ray.git
4. ray\thirdparty\download_thirdparty.bat

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
cluster, run `install_ray()` in the interpreter. The interpreter should block
until the installation has completed.
5. To check that the installation succeeded, you can ssh to each node, cd into
the directory `ray/test/`, and run the tests (e.g., `python runtest.py`).
6. Now that Ray has been installed, you can start the cluster (the scheduler,
object stores, and workers) with the command
`start_ray("/home/ubuntu/ray/scripts/default_worker.py")`, where the argument is
the path on each node in the cluster to the worker code that you would like to
use. The workers can be restarted with
`restart_workers("/home/ubuntu/ray/scripts/default_worker.py")`, for example if
you wish to update the application code running on the workers. The cluster
processes (the scheduler, the object stores, and the workers) can be stopped
with `stop_ray()`.
