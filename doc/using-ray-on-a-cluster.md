## Using Ray on a cluster

Running Ray on a cluster is still experimental.

Ray can be used in several ways. In addition to running on a single machine, Ray
is designed to run on a cluster of machines. This document is about how to use
Ray on a cluster.

### Launching a cluster on EC2

This section describes how to start a cluster on EC2. These instructions are
copied and adapted from https://github.com/amplab/spark-ec2.

#### Before you start

- Create an Amazon EC2 key pair for yourself. This can be done by logging into
your Amazon Web Services account through the [AWS
console](http://aws.amazon.com/console/), clicking Key Pairs on the left
sidebar, and creating and downloading a key. Make sure that you set the
permissions for the private key file to `600` (i.e. only you can read and write
it) so that `ssh` will work.
- Whenever you want to use the `ec2.py` script, set the environment variables
`AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` to your Amazon EC2 access key ID
and secret access key. These can be obtained from the [AWS
homepage](http://aws.amazon.com/) by clicking Account > Security Credentials >
Access Credentials.

#### Launching a Cluster

- Go into the `ray/scripts` directory.
- Run `python ec2.py -k <keypair> -i <key-file> -s <num-slaves> launch
<cluster-name>`, where `<keypair>` is the name of your EC2 key pair (that you
gave it when you created it), `<key-file>` is the private key file for your key
pair, `<num-slaves>` is the number of slave nodes to launch (try 1 at first),
and `<cluster-name>` is the name to give to your cluster.

    For example:

    ```bash
    export AWS_SECRET_ACCESS_KEY=AaBbCcDdEeFGgHhIiJjKkLlMmNnOoPpQqRrSsTtU
    export AWS_ACCESS_KEY_ID=ABCDEFG1234567890123
    python ec2.py --key-pair=awskey --identity-file=awskey.pem --region=us-west-1 launch my-ray-cluster
    ```

The following options are worth pointing out:

- `--instance-type=<instance-type>` can be used to specify an EC2 instance type
to use. For now, the script only supports 64-bit instance types, and the default
type is `m3.large` (which has 2 cores and 7.5 GB RAM).
- `--region=<ec2-region>` specifies an EC2 region in which to launch instances.
The default region is `us-east-1`.
- `--zone=<ec2-zone>` can be used to specify an EC2 availability zone to launch
instances in. Sometimes, you will get an error because there is not enough
capacity in one zone, and you should try to launch in another.
- `--spot-price=<price>` will launch the worker nodes as [Spot
Instances](http://aws.amazon.com/ec2/spot-instances/), bidding for the given
maximum price (in dollars).

### Getting started with Ray on a cluster

These instructions work on EC2, but they may require some modifications to run
on your own cluster. In particular, on EC2, running `sudo` does not require a
password, and we currently don't handle the case where a password is needed.

1. If you launched a cluster using the `ec2.py` script from the previous
section, then the file `ray/scripts/nodes.txt` will already have been created.
Otherwise, create a file `nodes.txt` of the IP addresses of the nodes in the
cluster. For example

        12.34.56.789
        12.34.567.89
The first node in the file is the "head" node. The scheduler will be started on
the head node, and the driver should run on the head node as well.

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
appropriate values. This assumes that you can connect to each IP address
`<ip-address>` in `nodes.txt` with the command
    ```
    ssh -i <key-file> <username>@<ip-address>
    ```
4. The previous command should open a Python interpreter. To install Ray on the
cluster, run `cluster.install_ray()` in the interpreter. The interpreter should
block until the installation has completed. The standard output from the nodes
will be redirected to your terminal.
5. To check that the installation succeeded, you can ssh to each node, cd into
the directory `ray/test/`, and run the tests (e.g., `python runtest.py`).
6. Create a directory (for example, `mkdir ~/example_ray_code`) containing the
worker `worker.py` code along with the code for any modules imported by
`worker.py`. For example,

    ```
    cp ray/scripts/default_worker.py ~/example_ray_code/worker.py
    cp ray/scripts/example_functions.py ~/example_ray_code/
    ```

7. Start the cluster (the scheduler, object stores, and workers) with the
command `cluster.start_ray("~/example_ray_code")`, where the second argument is
the local path to the worker code that you would like to use. This command will
copy the worker code to each node and will start the cluster. After completing
successfully, this command will print out a command that can be run on the head
node to attach a shell (the driver) to the cluster. For example,

    ```
    source "$RAY_HOME/setup-env.sh";
    python "$RAY_HOME/scripts/shell.py" --scheduler-address=12.34.56.789:10001 --objstore-address=12.34.56.789:20001 --worker-address=12.34.56.789:30001 --attach
    ```

8. Note that there are several more commands that can be run from within
`cluster.py`.

    - `cluster.install_ray()` - This pulls the Ray source code on each node,
      builds all of the third party libraries, and builds the project itself.
    - `cluster.start_ray(worker_directory, num_workers_per_node=10)` - This
      starts a scheduler process on the head node, and it starts an object store
      and some workers on each node.
    - `cluster.stop_ray()` - This shuts down the cluster (killing all of the
      processes).
    - `cluster.restart_workers(worker_directory, num_workers_per_node=10)` -
      This kills the current workers and starts new workers using the worker
      code from the given file. Currently, this can only run when there are no
      tasks currently executing on any of the workers.
    - `cluster.update_ray()` - This pulls the latest Ray source code and builds
      it.

### Running Ray on a cluster

Once you've started a Ray cluster using the above instructions, to actually use
Ray, ssh to the head node (the first node listed in the `nodes.txt` file) and
attach a shell to the already running cluster.
