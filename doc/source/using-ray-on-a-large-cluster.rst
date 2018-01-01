Using Ray on a Large Cluster
============================

.. note::

    Starting with Ray 0.4.0 if you're using AWS you can use the automated `setup commands <http://ray.readthedocs.io/en/latest/autoscaling.html>`__.

Deploying Ray on a cluster requires a bit of manual work. The instructions here
illustrate how to use parallel ssh commands to simplify the process of running
commands and scripts on many machines simultaneously.

Booting up a cluster on EC2
---------------------------

* Create an EC2 instance running Ray following instructions for
  `installation on Ubuntu`_.

    * Add any packages that you may need for running your application.
    * Install the pssh package: ``sudo apt-get install pssh``.
* `Create an AMI`_ with Ray installed and with whatever code and libraries you
  want on the cluster.
* Use the EC2 console to launch additional instances using the AMI you created.
* Configure the instance security groups so that they machines can all
  communicate with one another.

.. _`installation on Ubuntu`: http://ray.readthedocs.io/en/latest/install-on-ubuntu.html
.. _`Create an AMI`: http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/creating-an-ami-ebs.html

Deploying Ray on a Cluster
--------------------------

This section assumes that you have a cluster of machines running and that these
nodes have network connectivity to one another. It also assumes that Ray is
installed on each machine.

Additional assumptions:

* All of the following commands are run from a machine designated as
  the **head node**.
* The head node will run Redis and the global scheduler.
* The head node has ssh access to all other nodes.
* All nodes are accessible via ssh keys
* Ray is checked out on each node at the location ``$HOME/ray``.

**Note:** The commands below will probably need to be customized for your
specific setup.

Connect to the head node
~~~~~~~~~~~~~~~~~~~~~~~~

In order to initiate ssh commands from the cluster head node we suggest enabling
ssh agent forwarding. This will allow the session that you initiate with the
head node to connect to other nodes in the cluster to run scripts on them. You
can enable ssh forwarding by running the following command before connecting to
the head node (replacing ``<ssh-key>`` with the path to the private key that you
would use when logging in to the nodes in the cluster).

.. code-block:: bash

  ssh-add <ssh-key>

Now log in to the head node with the following command, where
``<head-node-public-ip>`` is the public IP address of the head node (just choose
one of the nodes to be the head node).

.. code-block:: bash

  ssh -A ubuntu@<head-node-public-ip>

Build a list of node IP addresses
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

On the head node, populate a file ``workers.txt`` with one IP address on each
line. Do not include the head node IP address in this file. These IP addresses
should typically be private network IP addresses, but any IP addresses which the
head node can use to ssh to worker nodes will work here. This should look
something like the following.

.. code-block:: bash

  172.31.27.16
  172.31.29.173
  172.31.24.132
  172.31.29.224

Confirm that you can ssh to all nodes
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: bash

  for host in $(cat workers.txt); do
    ssh -o "StrictHostKeyChecking no" $host uptime
  done

You may need to verify the host keys during this process. If so, run this step
again to verify that it worked. If you see a **permission denied** error, you
most likely forgot to run ``ssh-add <ssh-key>`` before connecting to the head
node.

Starting Ray
~~~~~~~~~~~~

**Start Ray on the head node**

On the head node, run the following:

.. code-block:: bash

  ray start --head --redis-port=6379


**Start Ray on the worker nodes**

Create a file ``start_worker.sh`` that contains something like the following:

.. code-block:: bash

  # Make sure the SSH session has the correct version of Python on its path.
  # You will probably have to change the line below.
  export PATH=/home/ubuntu/anaconda3/bin/:$PATH
  ray start --redis-address=<head-node-ip>:6379

This script, when run on the worker nodes, will start up Ray. You will need to
replace ``<head-node-ip>`` with the IP address that worker nodes will use to
connect to the head node (most likely a **private IP address**). In this
example we also export the path to the Python installation since our remote
commands will not be executing in a login shell.

**Warning:** You will probably need to manually export the correct path to
Python (you will need to change the first line of ``start_worker.sh`` to find
the version of Python that Ray was built against). This is necessary because the
``PATH`` environment variable used by ``parallel-ssh`` can differ from the
``PATH`` environment variable that gets set when you ``ssh`` to the machine.

**Warning:** If the ``parallel-ssh`` command below appears to hang or otherwise
fails, ``head-node-ip`` may need to be a private IP address instead of a public
IP address (e.g., if you are using EC2). It's also possible that you forgot to
run ``ssh-add <ssh-key>`` or that you forgot the ``-A`` flag when connecting to
the head node.

Now use ``parallel-ssh`` to start up Ray on each worker node.

.. code-block:: bash

  parallel-ssh -h workers.txt -P -I < start_worker.sh

Note that on some distributions the ``parallel-ssh`` command may be called
``pssh``.

**Verification**

Now you have started all of the Ray processes on each node. These include:

- Some worker processes on each machine.
- An object store on each machine.
- A local scheduler on each machine.
- Multiple Redis servers (on the head node).
- One global scheduler (on the head node).

To confirm that the Ray cluster setup is working, start up Python on one of the
nodes in the cluster and enter the following commands to connect to the Ray
cluster.

.. code-block:: python

  import ray
  ray.init(redis_address="<redis-address>")

Here ``<redis-address>`` should have the form ``<head-node-ip>:6379``.

Now you can define remote functions and execute tasks. For example, to verify
that the correct number of nodes have joined the cluster, you can run the
following.

.. code-block:: python

  import time

  @ray.remote
  def f():
      time.sleep(0.01)
      return ray.services.get_node_ip_address()

  # Get a list of the IP addresses of the nodes that have joined the cluster.
  set(ray.get([f.remote() for _ in range(1000)]))


Stopping Ray
~~~~~~~~~~~~

**Stop Ray on worker nodes**

Create a file ``stop_worker.sh`` that contains something like the following:

.. code-block:: bash

  # Make sure the SSH session has the correct version of Python on its path.
  # You will probably have to change the line below.
  export PATH=/home/ubuntu/anaconda3/bin/:$PATH
  ray stop

This script, when run on the worker nodes, will stop Ray. Note, you will need to
replace ``/home/ubuntu/anaconda3/bin/`` with the correct path to your Python
installation.

Now use ``parallel-ssh`` to stop Ray on each worker node.

.. code-block:: bash

  parallel-ssh -h workers.txt -P -I < stop_worker.sh

**Stop Ray on the head node**

.. code-block:: bash

  ray stop

Upgrading Ray
~~~~~~~~~~~~~

Ray remains under active development so you may at times want to upgrade the
cluster to take advantage of improvements and fixes.

**Create an upgrade script**

On the head node, create a file called ``upgrade.sh`` that contains the commands
necessary to upgrade Ray. It should look something like the following:

.. code-block:: bash

  # Make sure the SSH session has the correct version of Python on its path.
  # You will probably have to change the line below.
  export PATH=/home/ubuntu/anaconda3/bin/:$PATH
  # Do pushd/popd to make sure we end up in the same directory.
  pushd .
  # Upgrade Ray.
  cd ray
  git remote set-url origin https://github.com/ray-project/ray
  git checkout master
  git pull
  cd python
  python setup.py install --user
  popd

This script executes a series of git commands to update the Ray source code, then builds
and installs Ray.

**Stop Ray on the cluster**

Follow the instructions for `Stopping Ray`_.

**Run the upgrade script on the cluster**

First run the upgrade script on the head node. This will upgrade the head node
and help confirm that the upgrade script is working properly.

.. code-block:: bash

  bash upgrade.sh

Next run the upgrade script on the worker nodes.

.. code-block:: bash

  parallel-ssh -h workers.txt -P -t 0 -I < upgrade.sh

Note here that we use the ``-t 0`` option to set the timeout to infinite. You
may also want to use the ``-p`` flag, which controls the degree of parallelism
used by parallel ssh.

It is probably a good idea to ssh to one of the other nodes and verify that the
upgrade script ran as expected.

Sync Application Files to other nodes
-------------------------------------

If you are running an application that reads input files or uses python
libraries then you may find it useful to copy a directory on the head node to
the worker nodes.

You can do this using the ``parallel-rsync`` command:

.. code-block:: bash

  parallel-rsync -h workers.txt -r <workload-dir> /home/ubuntu/<workload-dir>

where ``<workload-dir>`` is the directory you want to synchronize. Note that the
destination argument for this command must represent an absolute path on the
worker node.

Troubleshooting
---------------

Problems with parallel-ssh
~~~~~~~~~~~~~~~~~~~~~~~~~~

If any of the above commands fail, verify that the head node has SSH access to
the other nodes by running

.. code-block:: bash

  for host in $(cat workers.txt); do
    ssh $host uptime
  done

If you get a permission denied error, then make sure you have SSH'ed to the head
node with agent forwarding enabled. This is done as follows.

.. code-block:: bash

  ssh-add <ssh-key>
  ssh -A ubuntu@<head-node-public-ip>
