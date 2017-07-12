Installation on Docker
======================

You can install Ray on any platform that runs Docker. We do not presently
publish Docker images for Ray, but you can build them yourself using the Ray
distribution.

Using Docker can streamline the build process and provide a reliable way to get
up and running quickly.

Install Docker
--------------

Mac, Linux, Windows platforms
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The Docker Platform release is available for Mac, Windows, and Linux platforms.
Please download the appropriate version from the `Docker website`_ and follow
the corresponding installation instructions. Linux user may find these
`alternate instructions`_ helpful.

.. _`Docker website`: https://www.docker.com/products/overview#/install_the_platform
.. _`alternate instructions`: https://www.digitalocean.com/community/tutorials/how-to-install-and-use-docker-on-ubuntu-16-04

Docker installation on EC2 with Ubuntu
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The instructions below show in detail how to prepare an Amazon EC2 instance
running Ubuntu 16.04 for use with Docker.

Apply initialize the package repository and apply system updates:

.. code-block:: bash

  sudo apt-get update
  sudo apt-get -y dist-upgrade

Install Docker and start the service:

.. code-block:: bash

  sudo apt-get install -y docker.io
  sudo service docker start


Add the ``ubuntu`` user to the ``docker`` group to allow running Docker commands
without sudo:

.. code-block:: bash

  sudo usermod -a -G docker ubuntu

Initiate a new login to gain group permissions (alternatively, log out and log
back in again):

.. code-block:: bash

  exec sudo su -l ubuntu

Confirm that docker is running:

.. code-block:: bash

  docker images

Should produce an empty table similar to the following:

.. code-block:: bash

  REPOSITORY          TAG                 IMAGE ID            CREATED             SIZE


Clone the Ray repository
------------------------

.. code-block:: bash

  git clone https://github.com/ray-project/ray.git


Build Docker images
-------------------

Run the script to create Docker images.

.. code-block:: bash

  cd ray
  ./build-docker.sh

This script creates several Docker images:

- The ``ray-project/deploy`` image is a self-contained copy of code and binaries
  suitable for end users.
- The ``ray-project/examples`` adds additional libraries for running examples.
- The ``ray-project/base-deps`` image builds from Ubuntu Xenial and includes
  Anaconda and other basic dependencies and can serve as a starting point for
  developers.

Review images by listing them:

.. code-block:: bash

  docker images

Output should look something like the following:

.. code-block:: bash

  REPOSITORY                          TAG                 IMAGE ID            CREATED             SIZE
  ray-project/examples                latest              7584bde65894        4 days ago          3.257 GB
  ray-project/deploy                  latest              970966166c71        4 days ago          2.899 GB
  ray-project/base-deps               latest              f45d66963151        4 days ago          2.649 GB
  ubuntu                              xenial              f49eec89601e        3 weeks ago         129.5 MB


Launch Ray in Docker
--------------------

Start out by launching the deployment container.

.. code-block:: bash

  docker run --shm-size=<shm-size> -t -i ray-project/deploy

Replace ``<shm-size>`` with a limit appropriate for your system, for example
``512M`` or ``2G``. The ``-t`` and ``-i`` options here are required to support
interactive use of the container.

**Note:** Ray requires a **large** amount of shared memory because each object
store keeps all of its objects in shared memory, so the amount of shared memory
will limit the size of the object store.

You should now see a prompt that looks something like:

.. code-block:: bash

  root@ebc78f68d100:/ray#


Test if the installation succeeded
----------------------------------

To test if the installation was successful, try running some tests. Within the
container shell enter the following commands:

.. code-block:: bash

  python test/runtest.py # This tests basic functionality.

You are now ready to continue with the `tutorial`_.

.. _`tutorial`: http://ray.readthedocs.io/en/latest/tutorial.html

Running examples in Docker
--------------------------

Ray includes a Docker image that includes dependencies necessary for running
some of the examples. This can be an easy way to see Ray in action on a variety
of workloads.

Launch the examples container.

.. code-block:: bash

  docker run --shm-size=1024m -t -i ray-project/examples

Hyperparameter optimization
~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: bash

  cd /ray/examples/hyperopt/
  python /ray/examples/hyperopt/hyperopt_simple.py

Batch L-BFGS
~~~~~~~~~~~~

.. code-block:: bash

  python /ray/examples/lbfgs/driver.py

Learning to play Pong
~~~~~~~~~~~~~~~~~~~~~

.. code-block:: bash

  python /ray/examples/rl_pong/driver.py
