Installation on Ubuntu
======================

Ray should work with Python 2 and Python 3. We have tested Ray on Ubuntu 14.04
and Ubuntu 16.04.

You can install Ray as follows.

.. code-block:: bash

  pip install ray

Building Ray from source
------------------------

If you want to use the latest version of Ray, you can build it from source.

Dependencies
~~~~~~~~~~~~

To build Ray, first install the following dependencies. We recommend using
`Anaconda`_.

.. _`Anaconda`: https://www.continuum.io/downloads

.. code-block:: bash

  sudo apt-get update
  sudo apt-get install -y cmake pkg-config build-essential autoconf curl libtool unzip python # we install python here because python2 is required to build the webui

  # If you are not using Anaconda, you need the following.
  sudo apt-get install python-dev  # For Python 2.
  sudo apt-get install python3-dev  # For Python 3.

  # If you are on Ubuntu 14.04, you need the following.
  pip install cmake

  pip install numpy funcsigs click colorama psutil redis flatbuffers cython


If you are using Anaconda, you may also need to run the following.

.. code-block:: bash

  conda install libgcc


Install Ray
~~~~~~~~~~~

Ray can be built from the repository as follows.

.. code-block:: bash

  git clone https://github.com/ray-project/ray.git
  cd ray/python
  python setup.py install  # Add --user if you see a permission denied error.

Alternatively, Ray can be built from the repository without cloning using pip.

.. code-block:: bash

    pip install git+https://github.com/ray-project/ray.git#subdirectory=python

Test if the installation succeeded
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To test if the installation was successful, try running some tests. This assumes
that you've cloned the git repository.

.. code-block:: bash

  python test/runtest.py

Cleaning the source tree
~~~~~~~~~~~~~~~~~~~~~~~~

The source tree can be cleaned by running

.. code-block:: bash

  git clean -f -f -x -d

in the ``ray/`` directory.
