Installation on Ubuntu
======================

Ray should work with Python 2 and Python 3. We have tested Ray on Ubuntu 14.04
and Ubuntu 16.04.

Dependencies
------------

To install Ray, first install the following dependencies. We recommend using
`Anaconda`_.

.. _`Anaconda`: https://www.continuum.io/downloads

.. code-block:: bash

  sudo apt-get update
  sudo apt-get install -y cmake build-essential autoconf curl libtool libboost-all-dev unzip

  # If you are not using Anaconda, you need the following.
  sudo apt-get install python-dev  # For Python 2.
  sudo apt-get install python3-dev  # For Python 3.

  pip install numpy cloudpickle funcsigs click colorama psutil redis flatbuffers


If you are using Anaconda, you may also need to run the following.

.. code-block:: bash

  conda install libgcc


Install Ray
-----------

Ray can be built from the repository as follows.

.. code-block:: bash

  git clone https://github.com/ray-project/ray.git
  cd ray/python
  python setup.py install --user


Test if the installation succeeded
----------------------------------

To test if the installation was successful, try running some tests. This assumes
that you've cloned the git repository.

.. code-block:: bash

  python test/runtest.py
