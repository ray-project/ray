Installation on Mac OS X
========================

Ray should work with Python 2 and Python 3. We have tested Ray on OS X 10.11 and
10.12.

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

  brew update
  brew install cmake pkg-config automake autoconf libtool boost openssl bison wget

  pip install cython

If you are using Anaconda, you may also need to run the following.

.. code-block:: bash

  conda install libgcc


Install Ray
~~~~~~~~~~~

Ray can be built from the repository as follows.

.. code-block:: bash

  git clone https://github.com/ray-project/ray.git
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

  python test/runtest.py

Cleaning the source tree
~~~~~~~~~~~~~~~~~~~~~~~~

The source tree can be cleaned by running

.. code-block:: bash

  git clean -f -f -x -d

in the ``ray/`` directory.
