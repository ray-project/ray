Installing Ray
==============

Ray should work with Python 2 and Python 3. We have tested Ray on Ubuntu 14.04, Ubuntu 16.04, Ubuntu 18.04,
MacOS 10.11, 10.12, 10.13, and 10.14.

Latest stable version
---------------------

You can install the latest stable version of Ray as follows.

.. code-block:: bash

  pip install -U ray  # also recommended: ray[debug]

Trying snapshots from master
----------------------------

Here are links to the latest wheels (which are built off of master). To install these wheels, run the following command:

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


.. _`Linux Python 3.7`: https://s3-us-west-2.amazonaws.com/ray-wheels/latest/ray-0.8.0.dev2-cp37-cp37m-manylinux1_x86_64.whl
.. _`Linux Python 3.6`: https://s3-us-west-2.amazonaws.com/ray-wheels/latest/ray-0.8.0.dev2-cp36-cp36m-manylinux1_x86_64.whl
.. _`Linux Python 3.5`: https://s3-us-west-2.amazonaws.com/ray-wheels/latest/ray-0.8.0.dev2-cp35-cp35m-manylinux1_x86_64.whl
.. _`Linux Python 2.7`: https://s3-us-west-2.amazonaws.com/ray-wheels/latest/ray-0.8.0.dev2-cp27-cp27mu-manylinux1_x86_64.whl
.. _`MacOS Python 3.7`: https://s3-us-west-2.amazonaws.com/ray-wheels/latest/ray-0.8.0.dev2-cp37-cp37m-macosx_10_6_intel.whl
.. _`MacOS Python 3.6`: https://s3-us-west-2.amazonaws.com/ray-wheels/latest/ray-0.8.0.dev2-cp36-cp36m-macosx_10_6_intel.whl
.. _`MacOS Python 3.5`: https://s3-us-west-2.amazonaws.com/ray-wheels/latest/ray-0.8.0.dev2-cp35-cp35m-macosx_10_6_intel.whl
.. _`MacOS Python 2.7`: https://s3-us-west-2.amazonaws.com/ray-wheels/latest/ray-0.8.0.dev2-cp27-cp27m-macosx_10_6_intel.whl


Building Ray from source
------------------------

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

Test if the installation succeeded
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To test if the installation was successful, try running some tests. This assumes
that you've cloned the git repository.

.. code-block:: bash

  python -m pytest -v python/ray/tests/test_mini.py

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
