Installing Ray
==============

.. tip:: Join our `community slack <https://forms.gle/9TSdDYUgxYs8SA9e8>`_ to discuss Ray!

Ray currently supports MacOS and Linux.
Windows wheels are now available, but :ref:`Windows support <windows-support>` is experimental and under development.

Latest stable version
---------------------

You can install the latest stable version of Ray as follows.

.. code-block:: bash

  pip install -U ray  # also recommended: ray[debug]

**Note for Windows Users:** To use Ray on Windows, Visual C++ runtime must be installed (see :ref:`Windows Dependencies <windows-dependencies>` section). If you run into any issues, please see the :ref:`Windows Support <windows-support>` section.

.. _install-nightlies:

Latest Snapshots (Nightlies)
----------------------------

Here are links to the latest wheels (which are built for each commit on the
master branch). To install these wheels, use the following ``pip`` command and wheels
instead of the ones above:

.. code-block:: bash

  pip install -U [link to wheel]


===================  ===================  ======================
       Linux                MacOS         Windows (experimental)
===================  ===================  ======================
`Linux Python 3.8`_  `MacOS Python 3.8`_  `Windows Python 3.8`_
`Linux Python 3.7`_  `MacOS Python 3.7`_  `Windows Python 3.7`_
`Linux Python 3.6`_  `MacOS Python 3.6`_  `Windows Python 3.6`_
===================  ===================  ======================

.. _`Linux Python 3.8`: https://s3-us-west-2.amazonaws.com/ray-wheels/latest/ray-0.9.0.dev0-cp38-cp38-manylinux1_x86_64.whl
.. _`Linux Python 3.7`: https://s3-us-west-2.amazonaws.com/ray-wheels/latest/ray-0.9.0.dev0-cp37-cp37m-manylinux1_x86_64.whl
.. _`Linux Python 3.6`: https://s3-us-west-2.amazonaws.com/ray-wheels/latest/ray-0.9.0.dev0-cp36-cp36m-manylinux1_x86_64.whl

.. _`MacOS Python 3.8`: https://s3-us-west-2.amazonaws.com/ray-wheels/latest/ray-0.9.0.dev0-cp38-cp38-macosx_10_13_x86_64.whl
.. _`MacOS Python 3.7`: https://s3-us-west-2.amazonaws.com/ray-wheels/latest/ray-0.9.0.dev0-cp37-cp37m-macosx_10_13_intel.whl
.. _`MacOS Python 3.6`: https://s3-us-west-2.amazonaws.com/ray-wheels/latest/ray-0.9.0.dev0-cp36-cp36m-macosx_10_13_intel.whl

.. _`Windows Python 3.8`: https://s3-us-west-2.amazonaws.com/ray-wheels/latest/ray-0.9.0.dev0-cp38-cp38-win_amd64.whl
.. _`Windows Python 3.7`: https://s3-us-west-2.amazonaws.com/ray-wheels/latest/ray-0.9.0.dev0-cp37-cp37m-win_amd64.whl
.. _`Windows Python 3.6`: https://s3-us-west-2.amazonaws.com/ray-wheels/latest/ray-0.9.0.dev0-cp36-cp36m-win_amd64.whl


Installing from a specific commit
---------------------------------

You can install the Ray wheels of any particular commit on ``master`` with the following template. You need to specify the commit hash, Ray version, Operating System, and Python version:

.. code-block:: bash

    pip install https://ray-wheels.s3-us-west-2.amazonaws.com/master/{COMMIT_HASH}/ray-{RAY_VERSION}-{PYTHON_VERSION}-{PYTHON_VERSION}m-{OS_VERSION}_intel.whl

For example, here are the Ray 0.9.0.dev0 wheels for Python 3.5, MacOS for commit ``a0ba4499ac645c9d3e82e68f3a281e48ad57f873``:

.. code-block:: bash

    pip install https://ray-wheels.s3-us-west-2.amazonaws.com/master/a0ba4499ac645c9d3e82e68f3a281e48ad57f873/ray-0.9.0.dev0-cp35-cp35m-macosx_10_13_intel.whl

.. _windows-support:

Windows Support
---------------

Windows support is currently limited and "alpha" quality.
Bugs, process/resource leaks, or other incompatibilities may exist under various scenarios.
Unusual, unattended, or production usage is **not** recommended.

To use Ray on Windows, the Visual C++ runtime must be installed (see :ref:`Windows Dependencies <windows-dependencies>` section).

If you encounter any issues, please try the following:

- Check the `Windows Known Issues <https://github.com/ray-project/ray/issues/9114>`_ page on GitHub to see the latest updates on Windows support.
- In the case that your issue has been addressed, try installing the :ref:`latest nightly wheels <install-nightlies>`.

If your issue has not yet been addressed, comment on the `Windows Known Issues <https://github.com/ray-project/ray/issues/9114>`_ page.

.. _windows-dependencies:

Windows Dependencies
~~~~~~~~~~~~~~~~~~~~

For Windows, ensure the latest `Visual C++ runtime`_ (`install link`_) is installed before using Ray.

Otherwise, you may receive an error similar to the following when Ray fails to find
the runtime library files (e.g. ``VCRUNTIME140_1.dll``):

.. code-block:: bash

  FileNotFoundError: Could not find module '_raylet.pyd' (or one of its dependencies).

.. _`Visual C++ Runtime`: https://support.microsoft.com/en-us/help/2977003/the-latest-supported-visual-c-downloads
.. _`install link`: https://aka.ms/vs/16/release/vc_redist.x64.exe


Installing Ray on Arch Linux
----------------------------

Note: Installing Ray on Arch Linux is not tested by the Project Ray developers.

Ray is available on Arch Linux via the Arch User Repository (`AUR`_) as
``python-ray``.

You can manually install the package by following the instructions on the
`Arch Wiki`_ or use an `AUR helper`_ like `yay`_ (recommended for ease of install)
as follows:

.. code-block:: bash

  yay -S python-ray

To discuss any issues related to this package refer to the comments section
on the AUR page of ``python-ray`` `here`_.

.. _`AUR`: https://wiki.archlinux.org/index.php/Arch_User_Repository
.. _`Arch Wiki`: https://wiki.archlinux.org/index.php/Arch_User_Repository#Installing_packages
.. _`AUR helper`: https://wiki.archlinux.org/index.php/Arch_User_Repository#Installing_packages
.. _`yay`: https://aur.archlinux.org/packages/yay
.. _`here`: https://aur.archlinux.org/packages/python-ray



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

However, should you need to build from source, follow :ref:`these instructions for building <building-ray>` Ray.



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
