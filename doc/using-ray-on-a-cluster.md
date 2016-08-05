# Using Ray on a cluster

Running Ray on a cluster is still experimental.

Ray can be used in several ways. In addition to running on a single machine, Ray
is designed to run on a cluster of machines. This document is about how to use
Ray on a cluster.

## Launching a cluster on EC2

This section describes how to start a cluster on EC2. These instructions are
copied and adapted from https://github.com/amplab/spark-ec2.

### Before you start

- Create an Amazon EC2 key pair for yourself. This can be done by logging into
your Amazon Web Services account through the [AWS
console](http://aws.amazon.com/console/), clicking Key Pairs on the left
sidebar, and creating and downloading a key. Make sure that you set the
permissions for the private key file to `600` (i.e. only you can read and write
it) so that `ssh` will work.
- Whenever you want to use the `ec2.py` script, set the environment variables
`AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` to your Amazon EC2 access key ID
and secret access key. These can be generated from the [AWS
homepage](http://aws.amazon.com/) by clicking My Account > Security Credentials >
Access Keys, or by [creating an IAM user](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_users_create.html).

### Launching a Cluster

- Install the required dependencies on the machine you will be using to run the
cluster launch scripts.
    ```
    sudo pip install --upgrade boto
    ```

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
    python ec2.py --key-pair=awskey \
                  --identity-file=awskey.pem \
                  --region=us-west-1 \
                  --instance-type=c4.4xlarge \
                  --spot-price=2.50 \
                  --slaves=1 \
                  launch my-ray-cluster
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
- `--slaves=<num-slaves>` will launch a cluster with `(1 + num_slaves)` instances.
The first instance is the head node, which in addition to hosting workers runs the
Ray scheduler and application driver programs.

## Getting started with Ray on a cluster

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
the head node, and the driver should run on the head node as well. If the nodes
have public and private IP addresses (as in the case of EC2 instances), you can
list the `<public-ip-address>, <private-ip-address>` in `nodes.txt` like

        12.34.56.789, 98.76.54.321
        12.34.567.89, 98.76.543.21
The `cluster.py` administrative script will use the public IP addresses to ssh
to the nodes. Ray will use the private IP addresses to send messages between the
nodes during execution.

2. Make sure that the nodes can all communicate with one another. On EC2, this
can be done by creating a new security group with the appropriate inbound and
outbound rules and adding all of the nodes in your cluster to that security
group. This is done automatically by the `ec2.py` script. If you have used the
`ec2.py` script you can log into the hosts with the username `ubuntu`.

3. From the `ray/scripts` directory, run something like

    ```
    python cluster.py --nodes=nodes.txt \
                      --key-file=awskey.pem \
                      --username=ubuntu
    ```
where you replace `nodes.txt`, `key.pem`, and `ubuntu` by the appropriate
values. This assumes that you can connect to each IP address `<ip-address>` in
`nodes.txt` with the command
    ```
    ssh -i <key-file> <username>@<ip-address>
    ```
4. The previous command should open a Python interpreter. To install Ray on the
cluster, run `cluster.install_ray()` in the interpreter. The interpreter should
block until the installation has completed. The standard output from the nodes
will be redirected to your terminal.
5. To check that the installation succeeded, you can ssh to each node and run
the tests.
    ```
    cd $HOME/ray/
    source setup-env.sh  # Add Ray to your Python path.
    python test/runtest.py  # This tests basic functionality.
    python test/array_test.py  # This tests some array libraries.
    ```

6. Start the cluster with `cluster.start_ray()`. If you would like to deploy
source code to it, you can pass in the local path to the directory that contains
your Python code. For example, `cluster.start_ray("~/example_ray_code")`. This
will copy your source code to each node on the cluster, placing it in a
directory on the PYTHONPATH.
The `cluster.start_ray` command will start the Ray scheduler, object stores, and
workers, and before finishing it will print instructions for connecting to the
cluster via ssh.

7. To connect to the cluster (either with a Python shell or with a script), ssh
to the cluster's head node (as described by the output of the
`cluster.start_ray` command. E.g.,
    ```
    The cluster has been started. You can attach to the cluster by sshing to the head node with the following command.

        ssh -i awskey.pem ubuntu@12.34.56.789

    Then run the following commands.

        cd $HOME/ray
        source $HOME/ray/setup-env.sh  # Add Ray to your Python path.

    Then within a Python interpreter, run the following commands.

        import ray
        ray.init(scheduler_address="98.76.54.321:10001", objstore_address="98.76.54.321:20001", driver_address="98.76.54.321:30001")
    ```

7. Note that there are several more commands that can be run from within
`cluster.py`.

    - `cluster.install_ray()` - This pulls the Ray source code on each node,
      builds all of the third party libraries, and builds the project itself.
    - `cluster.start_ray(user_source_directory=None, num_workers_per_node=10)` -
      This starts a scheduler process on the head node, and it starts an object
      store and some workers on each node.
    - `cluster.stop_ray()` - This shuts down the cluster (killing all of the
      processes).
    - `cluster.update_ray()` - This pulls the latest Ray source code and builds
      it.
