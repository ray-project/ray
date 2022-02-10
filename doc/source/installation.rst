.. _installation:

Installing Ray
==============

Ray currently supports Linux, MacOS and Windows.
Ray on Windows is currently in beta.

Official Releases
-----------------

You can install the latest official version of Ray as follows.

.. code-block:: bash

  pip install -U ray  # minimal install

  # To install Ray with support for the dashboard + cluster launcher, run
  # `pip install -U "ray[default]"`

To install Ray libraries:

.. code-block:: bash

  pip install -U "ray[tune]"  # installs Ray + dependencies for Ray Tune
  pip install -U "ray[rllib]"  # installs Ray + dependencies for Ray RLlib
  pip install -U "ray[serve]"  # installs Ray + dependencies for Ray Serve

.. _install-nightlies:

Daily Releases (Nightlies)
--------------------------

You can install the nightly Ray wheels via the following links. These daily releases are tested via automated tests but do not go through the full release process. To install these wheels, use the following ``pip`` command and wheels:

.. code-block:: bash

  pip uninstall -y ray # clean removal of previous install, otherwise version number may cause pip not to upgrade
  pip install -U LINK_TO_WHEEL.whl  # minimal install

  # To install Ray with support for the dashboard + cluster launcher, run
  # `pip install -U 'ray[default] @ LINK_TO_WHEEL.whl'`


===================  ===================  ======================
       Linux                MacOS         Windows (beta)
===================  ===================  ======================
`Linux Python 3.9`_  `MacOS Python 3.9`_  `Windows Python 3.9`_
`Linux Python 3.8`_  `MacOS Python 3.8`_  `Windows Python 3.8`_
`Linux Python 3.7`_  `MacOS Python 3.7`_  `Windows Python 3.7`_
`Linux Python 3.6`_  `MacOS Python 3.6`_  `Windows Python 3.6`_
===================  ===================  ======================

.. note::

  Python 3.9 support is currently experimental.

.. note::

  On Windows, support for multi-node Ray clusters is currently experimental and untested.
  If you run into issues please file a report at https://github.com/ray-project/ray/issues.

.. _`Linux Python 3.9`: https://s3-us-west-2.amazonaws.com/ray-wheels/latest/ray-2.0.0.dev0-cp39-cp39-manylinux2014_x86_64.whl
.. _`Linux Python 3.8`: https://s3-us-west-2.amazonaws.com/ray-wheels/latest/ray-2.0.0.dev0-cp38-cp38-manylinux2014_x86_64.whl
.. _`Linux Python 3.7`: https://s3-us-west-2.amazonaws.com/ray-wheels/latest/ray-2.0.0.dev0-cp37-cp37m-manylinux2014_x86_64.whl
.. _`Linux Python 3.6`: https://s3-us-west-2.amazonaws.com/ray-wheels/latest/ray-2.0.0.dev0-cp36-cp36m-manylinux2014_x86_64.whl

.. _`MacOS Python 3.9`: https://s3-us-west-2.amazonaws.com/ray-wheels/latest/ray-2.0.0.dev0-cp39-cp39-macosx_10_15_x86_64.whl
.. _`MacOS Python 3.8`: https://s3-us-west-2.amazonaws.com/ray-wheels/latest/ray-2.0.0.dev0-cp38-cp38-macosx_10_15_x86_64.whl
.. _`MacOS Python 3.7`: https://s3-us-west-2.amazonaws.com/ray-wheels/latest/ray-2.0.0.dev0-cp37-cp37m-macosx_10_15_intel.whl
.. _`MacOS Python 3.6`: https://s3-us-west-2.amazonaws.com/ray-wheels/latest/ray-2.0.0.dev0-cp36-cp36m-macosx_10_15_intel.whl

.. _`Windows Python 3.9`: https://s3-us-west-2.amazonaws.com/ray-wheels/latest/ray-2.0.0.dev0-cp39-cp39-win_amd64.whl
.. _`Windows Python 3.8`: https://s3-us-west-2.amazonaws.com/ray-wheels/latest/ray-2.0.0.dev0-cp38-cp38-win_amd64.whl
.. _`Windows Python 3.7`: https://s3-us-west-2.amazonaws.com/ray-wheels/latest/ray-2.0.0.dev0-cp37-cp37m-win_amd64.whl
.. _`Windows Python 3.6`: https://s3-us-west-2.amazonaws.com/ray-wheels/latest/ray-2.0.0.dev0-cp36-cp36m-win_amd64.whl


Installing from a specific commit
---------------------------------

You can install the Ray wheels of any particular commit on ``master`` with the following template. You need to specify the commit hash, Ray version, Operating System, and Python version:

.. code-block:: bash

    pip install https://s3-us-west-2.amazonaws.com/ray-wheels/master/{COMMIT_HASH}/ray-{RAY_VERSION}-{PYTHON_VERSION}-{PYTHON_VERSION}m-{OS_VERSION}.whl

For example, here are the Ray 2.0.0.dev0 wheels for Python 3.7, MacOS for commit ``ba6cebe30fab6925e5b2d9e859ad064d53015246``:

.. code-block:: bash

    pip install https://s3-us-west-2.amazonaws.com/ray-wheels/master/ba6cebe30fab6925e5b2d9e859ad064d53015246/ray-2.0.0.dev0-cp37-cp37m-macosx_10_15_intel.whl

There are minor variations to the format of the wheel filename; it's best to match against the format in the URLs listed in the :ref:`Nightlies section <install-nightlies>`.
Here's a summary of the variations:

* For Python 3.8 and 3.9, the ``m`` before the OS version should be deleted and the OS version for MacOS should read ``macosx_10_15_x86_64`` instead of ``macosx_10_15_intel``.

* For MacOS, commits predating August 7, 2021 will have ``macosx_10_13`` in the filename instad of ``macosx_10_15``.

.. _ray-install-java:

Install Ray Java with Maven
---------------------------
Before installing Ray Java with Maven, you should install Ray Python with `pip install -U ray` . Note that the versions of Ray Java and Ray Python must match.
Note that nightly Ray python wheels are also required if you want to install Ray Java snapshot version.

The latest Ray Java release can be found in `central repository <https://mvnrepository.com/artifact/io.ray>`__. To use the latest Ray Java release in your application, add the following entries in your ``pom.xml``:

.. code-block:: xml

    <dependency>
      <groupId>io.ray</groupId>
      <artifactId>ray-api</artifactId>
      <version>${ray.version}</version>
    </dependency>
    <dependency>
      <groupId>io.ray</groupId>
      <artifactId>ray-runtime</artifactId>
      <version>${ray.version}</version>
    </dependency>

The latest Ray Java snapshot can be found in `sonatype repository <https://oss.sonatype.org/#nexus-search;quick~io.ray>`__. To use the latest Ray Java snapshot in your application, add the following entries in your ``pom.xml``:

.. code-block:: xml

  <!-- only needed for snapshot version of ray -->
  <repositories>
    <repository>
      <id>sonatype</id>
      <url>https://oss.sonatype.org/content/repositories/snapshots/</url>
      <releases>
        <enabled>false</enabled>
      </releases>
      <snapshots>
        <enabled>true</enabled>
      </snapshots>
    </repository>
  </repositories>

  <dependencies>
    <dependency>
      <groupId>io.ray</groupId>
      <artifactId>ray-api</artifactId>
      <version>${ray.version}</version>
    </dependency>
    <dependency>
      <groupId>io.ray</groupId>
      <artifactId>ray-runtime</artifactId>
      <version>${ray.version}</version>
    </dependency>
  </dependencies>

.. note::

  When you run ``pip install`` to install Ray, Java jars are installed as well. The above dependencies are only used to build your Java code and to run your code in local mode.

  If you want to run your Java code in a multi-node Ray cluster, it's better to exclude Ray jars when packaging your code to avoid jar conficts if the versions (installed Ray with ``pip install`` and maven dependencies) don't match.

.. _apple-silcon-supprt:

M1 Mac (Apple Silicon) Support
------------------------------

Ray has experimental support for machines running Apple Silicon (such as M1 macs). To get started:

#. Install `miniforge <https://github.com/conda-forge/miniforge/releases/latest/download/Miniforge3-MacOSX-arm64.sh>`_.

   * ``wget https://github.com/conda-forge/miniforge/releases/latest/download/Miniforge3-MacOSX-arm64.sh``
   
   * ``bash Miniforge3-MacOSX-arm64.sh``
   
   * ``rm https://github.com/conda-forge/miniforge/releases/latest/download/Miniforge3-MacOSX-arm64.sh # Cleanup.``
   
#. Ensure you're using the miniforge environment (you should see (base) in your terminal).
   
   * ``source ~/.bash_profile``
   
   * ``conda activate``
   
#. Ensure that the ``grpcio`` package is installed via forge and **not pypi**. Grpcio currently requires special compilation flags, which pypi will _not_ correctly build with. Miniforge provides a prebuilt version of grpcio for M1 macs. 
   
   * ``pip uninstall grpcio; conda install grpcio``.

#. Install Ray as you normally would.

   * ``pip install ray``

.. note::

  At this time, Apple Silicon ray wheels are being published for **releases only**. As support stabilizes, nightly wheels will be published in the future.

.. _windows-support:

Windows Support
---------------

Windows support is currently in beta. Please submit any issues you encounter on
`GitHub <https://github.com/ray-project/ray/issues/>`_.

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

.. _ray_anaconda:

Installing Ray with Anaconda
----------------------------

If you use `Anaconda`_ (`installation instructions`_) and want to use Ray in a defined environment, e.g, ``ray``, use these commands:

.. code-block:: bash

  conda create --name ray
  conda activate ray
  conda install --name ray pip
  pip install ray

Use ``pip list`` to confirm that ``ray`` is installed.

.. _`Anaconda`: https://www.anaconda.com/
.. _`installation instructions`: https://docs.anaconda.com/anaconda/install/index.html




Building Ray from Source
------------------------

Installing from ``pip`` should be sufficient for most Ray users.

However, should you need to build from source, follow :ref:`these instructions for building <building-ray>` Ray.


.. _docker-images:

Docker Source Images
--------------------

Most users should pull a Docker image from the `Ray Docker Hub. <https://hub.docker.com/r/rayproject/>`_

- The ``rayproject/ray`` `image has ray and all required dependencies. It comes with anaconda and Python 3.7. <https://hub.docker.com/r/rayproject/ray>`_
- The ``rayproject/ray-ml`` `image has the above features as well as many additional libraries. <https://hub.docker.com/r/rayproject/ray-ml>`_
- The ``rayproject/base-deps`` and ``rayproject/ray-deps`` are for the linux and python dependencies respectively.

Image releases are `tagged` using the following format:


.. list-table::
   :widths: 25 50
   :header-rows: 1

   * - Tag
     - Description
   * - latest
     - The most recent Ray release.
   * - 1.x.x
     - A specific Ray release.
   * - nightly
     - The most recent Ray build (the most recent commit on Github ``master``)
   * - Git SHA
     - A specific nightly build (uses a SHA from the Github ``master``).


Some tags also have `variants` that add or change functionality:

.. list-table::
   :widths: 16 40
   :header-rows: 1

   * - Variant
     - Description
   * - -cpu
     - These are based off of an Ubuntu image.
   * - -cuXX
     - These are based off of an NVIDIA CUDA image with the specified CUDA version. They require the Nvidia Docker Runtime.
   * - -gpu
     - Aliases to a specific ``-cuXX`` tagged image.
   * - <no tag>
     - Aliases to ``-cpu`` tagged images. For ``ray-ml`` image, aliases to ``-gpu`` tagged image.


If you want to tweak some aspect of these images and build them locally, refer to the following script:

.. code-block:: bash

  cd ray
  ./build-docker.sh

Beyond creating the above Docker images, this script can also produce the following two images.

- The ``rayproject/development`` image has the ray source code included and is setup for development.
- The ``rayproject/examples`` image adds additional libraries for running examples.

Review images by listing them:

.. code-block:: bash

  docker images

Output should look something like the following:

.. code-block:: bash

  REPOSITORY                          TAG                 IMAGE ID            CREATED             SIZE
  rayproject/ray                      latest              7243a11ac068        2 days ago          1.11 GB
  rayproject/ray-deps                 latest              b6b39d979d73        8 days ago          996  MB
  rayproject/base-deps                latest              5606591eeab9        8 days ago          512  MB
  ubuntu                              focal               1e4467b07108        3 weeks ago         73.9 MB


Launch Ray in Docker
~~~~~~~~~~~~~~~~~~~~

Start out by launching the deployment container.

.. code-block:: bash

  docker run --shm-size=<shm-size> -t -i rayproject/ray

Replace ``<shm-size>`` with a limit appropriate for your system, for example
``512M`` or ``2G``. A good estimate for this is to use roughly 30% of your available memory (this is
what Ray uses internally for its Object Store). The ``-t`` and ``-i`` options here are required to support
interactive use of the container.

If you use a GPU version Docker image, remember to add ``--gpus all`` option. Replace ``<ray-version>`` with your target ray version in the following command:

.. code-block:: bash

  docker run --shm-size=<shm-size> -t -i --gpus all rayproject/ray:<ray-version>-gpu

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

Troubleshooting
---------------

If importing Ray (``python3 -c "import ray"``) in your development clone results
in this error:

.. code-block:: python

  Traceback (most recent call last):
    File "<string>", line 1, in <module>
    File ".../ray/python/ray/__init__.py", line 63, in <module>
      import ray._raylet  # noqa: E402
    File "python/ray/_raylet.pyx", line 98, in init ray._raylet
      import ray.memory_monitor as memory_monitor
    File ".../ray/python/ray/memory_monitor.py", line 9, in <module>
      import psutil  # noqa E402
    File ".../ray/python/ray/thirdparty_files/psutil/__init__.py", line 159, in <module>
      from . import _psosx as _psplatform
    File ".../ray/python/ray/thirdparty_files/psutil/_psosx.py", line 15, in <module>
      from . import _psutil_osx as cext
  ImportError: cannot import name '_psutil_osx' from partially initialized module 'psutil' (most likely due to a circular import) (.../ray/python/ray/thirdparty_files/psutil/__init__.py)

Then you should run the following commands:

.. code-block:: bash

  rm -rf python/ray/thirdparty_files/
  python3 -m pip install setproctitle
