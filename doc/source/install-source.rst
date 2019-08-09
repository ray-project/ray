Installing Ray from Source
==========================

If you want to use the latest version of Ray, you can build it from source.
Below, we have instructions for building from source for both Linux and MacOS.

Dependencies
~~~~~~~~~~~~

To build Ray, first install the following dependencies. We recommend using
`Anaconda`_.

.. _`Anaconda`: https://www.continuum.io/downloads

For Ubuntu, run the following commands:

.. code-block:: bash

  sudo apt-get update
  sudo apt-get install -y build-essential curl unzip psmisc

  # If you are not using Anaconda, you need the following.
  sudo apt-get install python-dev  # For Python 2.
  sudo apt-get install python3-dev  # For Python 3.

  pip install cython==0.29.0

For MacOS, run the following commands:

.. code-block:: bash

  brew update
  brew install wget

  pip install cython==0.29.0


If you are using Anaconda, you may also need to run the following.

.. code-block:: bash

  conda install libgcc


Install Ray
~~~~~~~~~~~

Ray can be built from the repository as follows.

.. code-block:: bash

  git clone https://github.com/ray-project/ray.git

  # Install Bazel.
  ray/ci/travis/install-bazel.sh

  cd ray/python
  pip install -e . --verbose  # Add --user if you see a permission denied error.

Alternatively, Ray can be built from the repository without cloning using pip.

.. code-block:: bash

    pip install git+https://github.com/ray-project/ray.git#subdirectory=python

Cleaning the source tree
~~~~~~~~~~~~~~~~~~~~~~~~

The source tree can be cleaned by running

.. code-block:: bash

  git clean -f -f -x -d

in the ``ray/`` directory. Warning: this command will delete all untracked files
and directories and will reset the repository to its checked out state.
For a shallower working directory cleanup, you may want to try:

.. code-block:: bash

  rm -rf ./build

under ``ray/``. Incremental builds should work as follows:

.. code-block:: bash

  pushd ./build && make && popd

under ``ray/``.

Docker Source Images
--------------------

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
~~~~~~~~~~~~~~~~~~~~

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
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To test if the installation was successful, try running some tests. This assumes
that you've cloned the git repository.

.. code-block:: bash

  python -m pytest -v python/ray/tests/test_mini.py


Troubleshooting installing Arrow
--------------------------------

Some candidate possibilities.

You have a different version of Flatbuffers installed
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Arrow pulls and builds its own copy of Flatbuffers, but if you already have
Flatbuffers installed, Arrow may find the wrong version. If a directory like
``/usr/local/include/flatbuffers`` shows up in the output, this may be the
problem. To solve it, get rid of the old version of flatbuffers.

There is some problem with Boost
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

If a message like ``Unable to find the requested Boost libraries`` appears when
installing Arrow, there may be a problem with Boost. This can happen if you
installed Boost using MacPorts. This is sometimes solved by using Brew instead.
