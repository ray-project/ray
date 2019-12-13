Installing Ray
==============

Ray supports Python 2 and Python 3 as well as MacOS and Linux. Windows support
is planned for the future.

Latest stable version
---------------------

You can install the latest stable version of Ray as follows.

.. code-block:: bash

  pip install -U ray  # also recommended: ray[debug]

Latest Snapshots (Nightlies)
----------------------------

Here are links to the latest wheels (which are built for each commit on the
master branch). To install these wheels, run the following command:

.. code-block:: bash

  pip install -U [link to wheel]


===================  ===================
       Linux                MacOS
===================  ===================
`Linux Python 3.7`_  `MacOS Python 3.7`_
`Linux Python 3.6`_  `MacOS Python 3.6`_
`Linux Python 3.5`_  `MacOS Python 3.5`_
`Linux Python 2.7`_  `MacOS Python 2.7`_
===================  ===================


.. _`Linux Python 3.7`: https://s3-us-west-2.amazonaws.com/ray-wheels/latest/ray-0.8.0.dev7-cp37-cp37m-manylinux1_x86_64.whl
.. _`Linux Python 3.6`: https://s3-us-west-2.amazonaws.com/ray-wheels/latest/ray-0.8.0.dev7-cp36-cp36m-manylinux1_x86_64.whl
.. _`Linux Python 3.5`: https://s3-us-west-2.amazonaws.com/ray-wheels/latest/ray-0.8.0.dev7-cp35-cp35m-manylinux1_x86_64.whl
.. _`Linux Python 2.7`: https://s3-us-west-2.amazonaws.com/ray-wheels/latest/ray-0.8.0.dev7-cp27-cp27mu-manylinux1_x86_64.whl
.. _`MacOS Python 3.7`: https://s3-us-west-2.amazonaws.com/ray-wheels/latest/ray-0.8.0.dev7-cp37-cp37m-macosx_10_6_intel.whl
.. _`MacOS Python 3.6`: https://s3-us-west-2.amazonaws.com/ray-wheels/latest/ray-0.8.0.dev7-cp36-cp36m-macosx_10_6_intel.whl
.. _`MacOS Python 3.5`: https://s3-us-west-2.amazonaws.com/ray-wheels/latest/ray-0.8.0.dev7-cp35-cp35m-macosx_10_6_intel.whl
.. _`MacOS Python 2.7`: https://s3-us-west-2.amazonaws.com/ray-wheels/latest/ray-0.8.0.dev7-cp27-cp27m-macosx_10_6_intel.whl

Installing Ray with Anaconda
----------------------------

If you use `Anaconda`_ and want to use Ray in a defined environment, e.g, ``ray``, use these commands: 

.. code-block:: bash

  conda create --name ray
  conda activate ray
  conda install --name ray pip
  pip install ray 

Use ``pip list`` to confirm that ``ray`` is installed.

.. _`Anaconda`: https://www.anaconda.com/


Building Ray from Source
------------------------

Installing from ``pip`` should be sufficient for most Ray users.

However, should you need to build from source, follow instructions below for
both Linux and MacOS.

Dependencies
~~~~~~~~~~~~

To build Ray, first install the following dependencies. We recommend using
`Anaconda`_.

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

  # Optionally build the dashboard (requires Node.js, see below for more information).
  pushd ray/python/ray/dashboard/client
  npm ci
  npm run build
  popd

  # Install Ray.
  cd ray/python
  pip install -e . --verbose  # Add --user if you see a permission denied error.


[Optional] Dashboard support
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

If you would like to use the dashboard, you will additionally need to install
`Node.js`_ and build the dashboard before installing Ray. The relevant build
steps are included in the installation instructions above.

.. _`Node.js`: https://nodejs.org/

The dashboard also requires:

-  Python 3
-  aiohttp
-  psutil

The latter two can be installed via pip:

.. code-block:: bash

  pip install aiohttp psutil
 
The dashboard is enabled by setting
``include_webui=True`` during initialization, i.e.

.. code-block:: python

  import ray
  ray.init(include_webui=True)


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
