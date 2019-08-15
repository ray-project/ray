Built-in Autoscaling
=====================

This document provides instructions for launching a Ray cluster either privately, on AWS, or on GCP.

The ``ray up`` command starts or updates a Ray cluster from your personal computer. Once the cluster is up, you can then SSH into it to run Ray programs.

Quick start (AWS)
-----------------

First, install boto (``pip install boto3``) and configure your AWS credentials in ``~/.aws/credentials``,
as described in `the boto docs <http://boto3.readthedocs.io/en/latest/guide/configuration.html>`__.

Then you're ready to go. The provided `ray/python/ray/autoscaler/aws/example-full.yaml <https://github.com/ray-project/ray/tree/master/python/ray/autoscaler/aws/example-full.yaml>`__ cluster config file will create a small cluster with a m5.large head node (on-demand) configured to autoscale up to two m5.large `spot workers <https://aws.amazon.com/ec2/spot/>`__.

Try it out by running these commands from your personal computer. Once the cluster is started, you can then
SSH into the head node, ``source activate tensorflow_p36``, and then run Ray programs with ``ray.init(redis_address="localhost:6379")``.

.. code-block:: bash

    # Create or update the cluster. When the command finishes, it will print
    # out the command that can be used to SSH into the cluster head node.
    $ ray up ray/python/ray/autoscaler/aws/example-full.yaml

    # Reconfigure autoscaling behavior without interrupting running jobs
    $ ray up ray/python/ray/autoscaler/aws/example-full.yaml \
        --max-workers=N --no-restart

    # Teardown the cluster
    $ ray down ray/python/ray/autoscaler/aws/example-full.yaml

Quick start (GCP)
-----------------

First, install the Google API client (``pip install google-api-python-client``), set up your GCP credentials, and create a new GCP project.

Then you're ready to go. The provided `ray/python/ray/autoscaler/gcp/example-full.yaml <https://github.com/ray-project/ray/tree/master/python/ray/autoscaler/gcp/example-full.yaml>`__ cluster config file will create a small cluster with a n1-standard-2 head node (on-demand) configured to autoscale up to two n1-standard-2 `preemptible workers <https://cloud.google.com/preemptible-vms/>`__. Note that you'll need to fill in your project id in those templates.

Try it out by running these commands from your personal computer. Once the cluster is started, you can then
SSH into the head node and then run Ray programs with ``ray.init(redis_address="localhost:6379")``.

.. code-block:: bash

    # Create or update the cluster. When the command finishes, it will print
    # out the command that can be used to SSH into the cluster head node.
    $ ray up ray/python/ray/autoscaler/gcp/example-full.yaml

    # Reconfigure autoscaling behavior without interrupting running jobs
    $ ray up ray/python/ray/autoscaler/gcp/example-full.yaml \
        --max-workers=N --no-restart

    # Teardown the cluster
    $ ray down ray/python/ray/autoscaler/gcp/example-full.yaml

Quick start (Private Cluster)
-----------------------------

This is used when you have a list of machine IP addresses to connect in a Ray cluster. You can get started by filling out the fields in the provided `ray/python/ray/autoscaler/local/example-full.yaml <https://github.com/ray-project/ray/tree/master/python/ray/autoscaler/local/example-full.yaml>`__.
Be sure to specify the proper ``head_ip``, list of ``worker_ips``, and the ``ssh_user`` field.

Try it out by running these commands from your personal computer. Once the cluster is started, you can then
SSH into the head node and then run Ray programs with ``ray.init(redis_address="localhost:6379")``.

.. code-block:: bash

    # Create or update the cluster. When the command finishes, it will print
    # out the command that can be used to SSH into the cluster head node.
    $ ray up ray/python/ray/autoscaler/local/example-full.yaml

    # Reconfigure autoscaling behavior without interrupting running jobs
    $ ray up ray/python/ray/autoscaler/local/example-full.yaml \
        --max-workers=N --no-restart

    # Teardown the cluster
    $ ray down ray/python/ray/autoscaler/local/example-full.yaml

Running commands on new and existing clusters
---------------------------------------------

You can use ``ray exec`` to conveniently run commands on clusters. Note that scripts you run should connect to Ray via ``ray.init(redis_address="localhost:6379")``.

.. code-block:: bash

    # Run a command on the cluster
    $ ray exec cluster.yaml 'echo "hello world"'

    # Run a command on the cluster, starting it if needed
    $ ray exec cluster.yaml 'echo "hello world"' --start

    # Run a command on the cluster, stopping the cluster after it finishes
    $ ray exec cluster.yaml 'echo "hello world"' --stop

    # Run a command on a new cluster called 'experiment-1', stopping it after
    $ ray exec cluster.yaml 'echo "hello world"' \
        --start --stop --cluster-name experiment-1

    # Run a command in a detached tmux session
    $ ray exec cluster.yaml 'echo "hello world"' --tmux

    # Run a command in a screen (experimental)
    $ ray exec cluster.yaml 'echo "hello world"' --screen

You can also use ``ray submit`` to execute Python scripts on clusters. This will ``rsync`` the designated file onto the cluster and execute it with the given arguments.

.. code-block:: bash

    # Run a Python script in a detached tmux session
    $ ray submit cluster.yaml --tmux --start --stop tune_experiment.py


Attaching to the cluster
------------------------

You can use ``ray attach`` to attach to an interactive console on the cluster.

.. code-block:: bash

    # Open a screen on the cluster
    $ ray attach cluster.yaml

    # Open a screen on a new cluster called 'session-1'
    $ ray attach cluster.yaml --start --cluster-name=session-1

    # Attach to tmux session on cluster (creates a new one if none available)
    $ ray attach cluster.yaml --tmux


Port-forwarding applications
----------------------------

To run connect to applications running on the cluster (e.g. Jupyter notebook) using a web browser, you can use the port-forward option for ``ray exec``. The local port opened is the same as the remote port:

.. code-block:: bash

    $ ray exec cluster.yaml --port-forward=8899 'source ~/anaconda3/bin/activate tensorflow_p36 && jupyter notebook --port=8899'

Manually synchronizing files
----------------------------

To download or upload files to the cluster head node, use ``ray rsync_down`` or ``ray rsync_up``:

.. code-block:: bash

    $ ray rsync_down cluster.yaml '/path/on/cluster' '/local/path'
    $ ray rsync_up cluster.yaml '/local/path' '/path/on/cluster'

Updating your cluster
---------------------

When you run ``ray up`` with an existing cluster, the command checks if the local configuration differs from the applied configuration of the cluster. This includes any changes to synced files specified in the ``file_mounts`` section of the config. If so, the new files and config will be uploaded to the cluster. Following that, Ray services will be restarted.

You can also run ``ray up`` to restart a cluster if it seems to be in a bad state (this will restart all Ray services even if there are no config changes).

If you don't want the update to restart services (e.g. because the changes don't require a restart), pass ``--no-restart`` to the update call.

Security
--------

By default, the nodes will be launched into their own security group, with traffic allowed only between nodes in the same group. A new SSH key will also be created and saved to your local machine for access to the cluster.

Autoscaling
-----------

Ray clusters come with a load-based auto-scaler. When cluster resource usage exceeds a configurable threshold (80% by default), new nodes will be launched up the specified ``max_workers`` limit. When nodes are idle for more than a timeout, they will be removed, down to the ``min_workers`` limit. The head node is never removed.

The default idle timeout is 5 minutes. This is to prevent excessive node churn which could impact performance and increase costs (in AWS / GCP there is a minimum billing charge of 1 minute per instance, after which usage is billed by the second).

Monitoring cluster status
-------------------------

You can monitor cluster usage and auto-scaling status by tailing the autoscaling
logs in ``/tmp/ray/session_*/logs/monitor*``.

The Ray autoscaler also reports per-node status in the form of instance tags. In your cloud provider console, you can click on a Node, go the the "Tags" pane, and add the ``ray-node-status`` tag as a column. This lets you see per-node statuses at a glance:

.. image:: autoscaler-status.png

Customizing cluster setup
-------------------------

You are encouraged to copy the example YAML file and modify it to your needs. This may include adding additional setup commands to install libraries or sync local data files.

.. note:: After you have customized the nodes, it is also a good idea to create a new machine image and use that in the config file. This reduces worker setup time, improving the efficiency of auto-scaling.

The setup commands you use should ideally be *idempotent*, that is, can be run more than once. This allows Ray to update nodes after they have been created. You can usually make commands idempotent with small modifications, e.g. ``git clone foo`` can be rewritten as ``test -e foo || git clone foo`` which checks if the repo is already cloned first.

Most of the example YAML file is optional. Here is a `reference minimal YAML file <https://github.com/ray-project/ray/tree/master/python/ray/autoscaler/aws/example-minimal.yaml>`__, and you can find the defaults for `optional fields in this YAML file <https://github.com/ray-project/ray/tree/master/python/ray/autoscaler/aws/example-full.yaml>`__.

Syncing git branches
--------------------

A common use case is syncing a particular local git branch to all workers of the cluster. However, if you just put a `git checkout <branch>` in the setup commands, the autoscaler won't know when to rerun the command to pull in updates. There is a nice workaround for this by including the git SHA in the input (the hash of the file will change if the branch is updated):

.. code-block:: yaml

    file_mounts: {
        "/tmp/current_branch_sha": "/path/to/local/repo/.git/refs/heads/<YOUR_BRANCH_NAME>",
    }

    setup_commands:
        - test -e <REPO_NAME> || git clone https://github.com/<REPO_ORG>/<REPO_NAME>.git
        - cd <REPO_NAME> && git fetch && git checkout `cat /tmp/current_branch_sha`

This tells ``ray up`` to sync the current git branch SHA from your personal computer to a temporary file on the cluster (assuming you've pushed the branch head already). Then, the setup commands read that file to figure out which SHA they should checkout on the nodes. Note that each command runs in its own session. The final workflow to update the cluster then becomes just this:

1. Make local changes to a git branch
2. Commit the changes with ``git commit`` and ``git push``
3. Update files on your Ray cluster with ``ray up``

Common cluster configurations
-----------------------------

The ``example-full.yaml`` configuration is enough to get started with Ray, but for more compute intensive workloads you will want to change the instance types to e.g. use GPU or larger compute instance by editing the yaml file. Here are a few common configurations:

**GPU single node**: use Ray on a single large GPU instance.

.. code-block:: yaml

    max_workers: 0
    head_node:
        InstanceType: p2.8xlarge

**Docker**: Specify docker image. This executes all commands on all nodes in the docker container,
and opens all the necessary ports to support the Ray cluster. It will also automatically install
Docker if Docker is not installed. This currently does not have GPU support.

.. code-block:: yaml

    docker:
        image: tensorflow/tensorflow:1.5.0-py3
        container_name: ray_docker

**Mixed GPU and CPU nodes**: for RL applications that require proportionally more
CPU than GPU resources, you can use additional CPU workers with a GPU head node.

.. code-block:: yaml

    max_workers: 10
    head_node:
        InstanceType: p2.8xlarge
    worker_nodes:
        InstanceType: m4.16xlarge

**Autoscaling CPU cluster**: use a small head node and have Ray auto-scale
workers as needed. This can be a cost-efficient configuration for clusters with
bursty workloads. You can also request spot workers for additional cost savings.

.. code-block:: yaml

    min_workers: 0
    max_workers: 10
    head_node:
        InstanceType: m4.large
    worker_nodes:
        InstanceMarketOptions:
            MarketType: spot
        InstanceType: m4.16xlarge

**Autoscaling GPU cluster**: similar to the autoscaling CPU cluster, but
with GPU worker nodes instead.

.. code-block:: yaml

    min_workers: 1  # must have at least 1 GPU worker (issue #2106)
    max_workers: 10
    head_node:
        InstanceType: m4.large
    worker_nodes:
        InstanceMarketOptions:
            MarketType: spot
        InstanceType: p2.xlarge


External Node Provider
--------------------------

Ray also supports external node providers (check `node_provider.py <https://github.com/ray-project/ray/tree/master/python/ray/autoscaler/node_provider.py>`__ implementation).
You can specify the external node provider using the yaml config:

.. code-block:: yaml

    provider:
        type: external
        module: mypackage.myclass

The module needs to be in the format `package.provider_class` or `package.sub_package.provider_class`.

Additional Cloud providers
--------------------------

To use Ray autoscaling on other Cloud providers or cluster management systems, you can implement the ``NodeProvider`` interface (~100 LOC) and register it in `node_provider.py <https://github.com/ray-project/ray/tree/master/python/ray/autoscaler/node_provider.py>`__. Contributions are welcome!

Questions or Issues?
--------------------

You can post questions or issues or feedback through the following channels:

1. `ray-dev@googlegroups.com`_: For discussions about development or any general
   questions and feedback.
2. `StackOverflow`_: For questions about how to use Ray.
3. `GitHub Issues`_: For bug reports and feature requests.

.. _`ray-dev@googlegroups.com`: https://groups.google.com/forum/#!forum/ray-dev
.. _`StackOverflow`: https://stackoverflow.com/questions/tagged/ray
.. _`GitHub Issues`: https://github.com/ray-project/ray/issues
