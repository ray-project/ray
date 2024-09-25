.. _installation:

Installing Ray
==============

.. raw:: html

    <a id="try-anyscale-quickstart-install-ray" target="_blank" href="https://console.anyscale.com/register/ha?utm_source=ray_docs&utm_medium=docs&utm_campaign=installing_ray&redirectTo=/v2/template-preview/workspace-intro">
      <img src="../_static/img/quickstart-with-ray.svg" alt="Run Quickstart on Anyscale" />
      <br/><br/>
    </a>

Ray currently officially supports x86_64, aarch64 (ARM) for Linux, and Apple silicon (M1) hardware.
Ray on Windows is currently in beta.

Official Releases
-----------------

From Wheels
~~~~~~~~~~~
You can install the latest official version of Ray from PyPI on Linux, Windows,
and macOS by choosing the option that best matches your use case.

.. tab-set::

    .. tab-item:: Recommended

        **For machine learning applications**

        .. code-block:: shell

          pip install -U "ray[data,train,tune,serve]"

          # For reinforcement learning support, install RLlib instead.
          # pip install -U "ray[rllib]"

        **For general Python applications**

        .. code-block:: shell

          pip install -U "ray[default]"

          # If you don't want Ray Dashboard or Cluster Launcher, install Ray with minimal dependencies instead.
          # pip install -U "ray"

    .. tab-item:: Advanced

        .. list-table::
          :widths: 2 3
          :header-rows: 1

          * - Command
            - Installed components
          * - `pip install -U "ray"`
            - Core
          * - `pip install -U "ray[default]"`
            - Core, Dashboard, Cluster Launcher
          * - `pip install -U "ray[data]"`
            - Core, Data
          * - `pip install -U "ray[train]"`
            - Core, Train
          * - `pip install -U "ray[tune]"`
            - Core, Tune
          * - `pip install -U "ray[serve]"`
            - Core, Dashboard, Cluster Launcher, Serve
          * - `pip install -U "ray[serve-grpc]"`
            - Core, Dashboard, Cluster Launcher, Serve with gRPC support
          * - `pip install -U "ray[rllib]"`
            - Core, Tune, RLlib
          * - `pip install -U "ray[all]"`
            - Core, Dashboard, Cluster Launcher, Data, Train, Tune, Serve, RLlib

        .. tip::

          You can combine installation extras.
          For example, to install Ray with Dashboard, Cluster Launcher, and Train support, you can run:

          .. code-block:: shell

            pip install -U "ray[default,train]"

.. _install-nightlies:

Daily Releases (Nightlies)
--------------------------

You can install the nightly Ray wheels via the following links. These daily releases are tested via automated tests but do not go through the full release process. To install these wheels, use the following ``pip`` command and wheels:

.. code-block:: bash

  # Clean removal of previous install
  pip uninstall -y ray
  # Install Ray with support for the dashboard + cluster launcher
  pip install -U "ray[default] @ LINK_TO_WHEEL.whl"

  # Install Ray with minimal dependencies
  # pip install -U LINK_TO_WHEEL.whl

.. tab-set::

    .. tab-item:: Linux

        =============================================== ================================================
               Linux (x86_64)                                   Linux (arm64/aarch64)
        =============================================== ================================================
        `Linux Python 3.9 (x86_64)`_                     `Linux Python 3.9 (aarch64)`_
        `Linux Python 3.10 (x86_64)`_                    `Linux Python 3.10 (aarch64)`_
        `Linux Python 3.11 (x86_64)`_                    `Linux Python 3.11 (aarch64)`_
        `Linux Python 3.12 (x86_64)`_                    `Linux Python 3.12 (aarch64)`_
        =============================================== ================================================

    .. tab-item:: MacOS

        ============================================  ==============================================
         MacOS (x86_64)                                MacOS (arm64)
        ============================================  ==============================================
        `MacOS Python 3.9 (x86_64)`_                   `MacOS Python 3.9 (arm64)`_
        `MacOS Python 3.10 (x86_64)`_                  `MacOS Python 3.10 (arm64)`_
        `MacOS Python 3.11 (x86_64)`_                  `MacOS Python 3.11 (arm64)`_
        `MacOS Python 3.12 (x86_64)`_                  `MacOS Python 3.12 (arm64)`_
        ============================================  ==============================================

    .. tab-item:: Windows (beta)

        .. list-table::
           :header-rows: 1

           * - Windows (beta)
           * - `Windows Python 3.9`_
           * - `Windows Python 3.10`_
           * - `Windows Python 3.11`_
           * - `Windows Python 3.12`_

.. note::

  On Windows, support for multi-node Ray clusters is currently experimental and untested.
  If you run into issues please file a report at https://github.com/ray-project/ray/issues.

.. note::

  :ref:`Usage stats <ref-usage-stats>` collection is enabled by default (can be :ref:`disabled <usage-disable>`) for nightly wheels including both local clusters started via ``ray.init()`` and remote clusters via cli.

.. note::

  .. If you change the list of wheel links below, remember to update `get_wheel_filename()` in  `https://github.com/ray-project/ray/blob/master/python/ray/_private/utils.py`.

.. _`Linux Python 3.9 (x86_64)`: https://s3-us-west-2.amazonaws.com/ray-wheels/latest/ray-3.0.0.dev0-cp39-cp39-manylinux2014_x86_64.whl
.. _`Linux Python 3.10 (x86_64)`: https://s3-us-west-2.amazonaws.com/ray-wheels/latest/ray-3.0.0.dev0-cp310-cp310-manylinux2014_x86_64.whl
.. _`Linux Python 3.11 (x86_64)`: https://s3-us-west-2.amazonaws.com/ray-wheels/latest/ray-3.0.0.dev0-cp311-cp311-manylinux2014_x86_64.whl
.. _`Linux Python 3.12 (x86_64)`: https://s3-us-west-2.amazonaws.com/ray-wheels/latest/ray-3.0.0.dev0-cp312-cp312-manylinux2014_x86_64.whl

.. _`Linux Python 3.9 (aarch64)`: https://s3-us-west-2.amazonaws.com/ray-wheels/latest/ray-3.0.0.dev0-cp39-cp39-manylinux2014_aarch64.whl
.. _`Linux Python 3.10 (aarch64)`: https://s3-us-west-2.amazonaws.com/ray-wheels/latest/ray-3.0.0.dev0-cp310-cp310-manylinux2014_aarch64.whl
.. _`Linux Python 3.11 (aarch64)`: https://s3-us-west-2.amazonaws.com/ray-wheels/latest/ray-3.0.0.dev0-cp311-cp311-manylinux2014_aarch64.whl
.. _`Linux Python 3.12 (aarch64)`: https://s3-us-west-2.amazonaws.com/ray-wheels/latest/ray-3.0.0.dev0-cp312-cp312-manylinux2014_aarch64.whl


.. _`MacOS Python 3.9 (x86_64)`: https://s3-us-west-2.amazonaws.com/ray-wheels/latest/ray-3.0.0.dev0-cp39-cp39-macosx_10_15_x86_64.whl
.. _`MacOS Python 3.10 (x86_64)`: https://s3-us-west-2.amazonaws.com/ray-wheels/latest/ray-3.0.0.dev0-cp310-cp310-macosx_10_15_x86_64.whl
.. _`MacOS Python 3.11 (x86_64)`: https://s3-us-west-2.amazonaws.com/ray-wheels/latest/ray-3.0.0.dev0-cp311-cp311-macosx_10_15_x86_64.whl
.. _`MacOS Python 3.12 (x86_64)`: https://s3-us-west-2.amazonaws.com/ray-wheels/latest/ray-3.0.0.dev0-cp312-cp312-macosx_10_15_x86_64.whl

.. _`MacOS Python 3.9 (arm64)`: https://s3-us-west-2.amazonaws.com/ray-wheels/latest/ray-3.0.0.dev0-cp39-cp39-macosx_11_0_arm64.whl
.. _`MacOS Python 3.10 (arm64)`: https://s3-us-west-2.amazonaws.com/ray-wheels/latest/ray-3.0.0.dev0-cp310-cp310-macosx_11_0_arm64.whl
.. _`MacOS Python 3.11 (arm64)`: https://s3-us-west-2.amazonaws.com/ray-wheels/latest/ray-3.0.0.dev0-cp311-cp311-macosx_11_0_arm64.whl
.. _`MacOS Python 3.12 (arm64)`: https://s3-us-west-2.amazonaws.com/ray-wheels/latest/ray-3.0.0.dev0-cp312-cp312-macosx_11_0_arm64.whl


.. _`Windows Python 3.9`: https://s3-us-west-2.amazonaws.com/ray-wheels/latest/ray-3.0.0.dev0-cp39-cp39-win_amd64.whl
.. _`Windows Python 3.10`: https://s3-us-west-2.amazonaws.com/ray-wheels/latest/ray-3.0.0.dev0-cp310-cp310-win_amd64.whl
.. _`Windows Python 3.11`: https://s3-us-west-2.amazonaws.com/ray-wheels/latest/ray-3.0.0.dev0-cp311-cp311-win_amd64.whl
.. _`Windows Python 3.12`: https://s3-us-west-2.amazonaws.com/ray-wheels/latest/ray-3.0.0.dev0-cp312-cp312-win_amd64.whl

Installing from a specific commit
---------------------------------

You can install the Ray wheels of any particular commit on ``master`` with the following template. You need to specify the commit hash, Ray version, Operating System, and Python version:

.. code-block:: bash

    pip install https://s3-us-west-2.amazonaws.com/ray-wheels/master/{COMMIT_HASH}/ray-{RAY_VERSION}-{PYTHON_VERSION}-{PYTHON_VERSION}-{OS_VERSION}.whl

For example, here are the Ray 3.0.0.dev0 wheels for Python 3.9, MacOS for commit ``4f2ec46c3adb6ba9f412f09a9732f436c4a5d0c9``:

.. code-block:: bash

    pip install https://s3-us-west-2.amazonaws.com/ray-wheels/master/4f2ec46c3adb6ba9f412f09a9732f436c4a5d0c9/ray-3.0.0.dev0-cp39-cp39-macosx_10_15_x86_64.whl

There are minor variations to the format of the wheel filename; it's best to match against the format in the URLs listed in the :ref:`Nightlies section <install-nightlies>`.
Here's a summary of the variations:

* For MacOS, commits predating August 7, 2021 will have ``macosx_10_13`` in the filename instead of ``macosx_10_15``.

.. _apple-silcon-supprt:

M1 Mac (Apple Silicon) Support
------------------------------

Ray supports machines running Apple Silicon (such as M1 macs).
Multi-node clusters are untested. To get started with local Ray development:

#. Install `miniforge <https://github.com/conda-forge/miniforge/releases/latest/download/Miniforge3-MacOSX-arm64.sh>`_.

   * ``wget https://github.com/conda-forge/miniforge/releases/latest/download/Miniforge3-MacOSX-arm64.sh``

   * ``bash Miniforge3-MacOSX-arm64.sh``

   * ``rm Miniforge3-MacOSX-arm64.sh # Cleanup.``

#. Ensure you're using the miniforge environment (you should see (base) in your terminal).

   * ``source ~/.bash_profile``

   * ``conda activate``

#. Install Ray as you normally would.

   * ``pip install ray``

.. _windows-support:

Windows Support
---------------

Windows support is in Beta. Ray supports running on Windows with the following caveats (only the first is
Ray-specific, the rest are true anywhere Windows is used):

* Multi-node Ray clusters are untested.

* Filenames are tricky on Windows and there still may be a few places where Ray
  assumes UNIX filenames rather than Windows ones. This can be true in downstream
  packages as well.

* Performance on Windows is known to be slower since opening files on Windows
  is considerably slower than on other operating systems. This can affect logging.

* Windows does not have a copy-on-write forking model, so spinning up new
  processes can require more memory.

Submit any issues you encounter to
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

Installing From conda-forge
---------------------------
Ray can also be installed as a conda package on Linux and Windows.

.. code-block:: bash

  # also works with mamba
  conda create -c conda-forge python=3.9 -n ray
  conda activate ray

  # Install Ray with support for the dashboard + cluster launcher
  conda install -c conda-forge "ray-default"

  # Install Ray with minimal dependencies
  # conda install -c conda-forge ray

To install Ray libraries, use ``pip`` as above or ``conda``/``mamba``.

.. code-block:: bash

  conda install -c conda-forge "ray-data"   # installs Ray + dependencies for Ray Data
  conda install -c conda-forge "ray-train"  # installs Ray + dependencies for Ray Train
  conda install -c conda-forge "ray-tune"   # installs Ray + dependencies for Ray Tune
  conda install -c conda-forge "ray-serve"  # installs Ray + dependencies for Ray Serve
  conda install -c conda-forge "ray-rllib"  # installs Ray + dependencies for Ray RLlib

For a complete list of available ``ray`` libraries on Conda-forge, have a look
at https://anaconda.org/conda-forge/ray-default

.. note::

  Ray conda packages are maintained by the community, not the Ray team. While
  using a conda environment, it is recommended to install Ray from PyPi using
  `pip install ray` in the newly created environment.

Building Ray from Source
------------------------

Installing from ``pip`` should be sufficient for most Ray users.

However, should you need to build from source, follow :ref:`these instructions for building <building-ray>` Ray.


.. _docker-images:

Docker Source Images
--------------------

Users can pull a Docker image from the ``rayproject/ray`` `Docker Hub repository <https://hub.docker.com/r/rayproject/ray>`__.
The images include Ray and all required dependencies. It comes with anaconda and various versions of Python.

Images are `tagged` with the format ``{Ray version}[-{Python version}][-{Platform}]``. ``Ray version`` tag can be one of the following:

.. list-table::
   :widths: 25 50
   :header-rows: 1

   * - Ray version tag
     - Description
   * - latest
     - The most recent Ray release.
   * - x.y.z
     - A specific Ray release, e.g. 2.31.0
   * - nightly
     - The most recent Ray development build (a recent commit from Github ``master``)

The optional ``Python version`` tag specifies the Python version in the image. All Python versions supported by Ray are available, e.g. ``py39``, ``py310`` and ``py311``. If unspecified, the tag points to an image of the lowest Python version that the Ray version supports.

The optional ``Platform`` tag specifies the platform where the image is intended for:

.. list-table::
   :widths: 16 40
   :header-rows: 1

   * - Platform tag
     - Description
   * - -cpu
     - These are based off of an Ubuntu image.
   * - -cuXX
     - These are based off of an NVIDIA CUDA image with the specified CUDA version. They require the Nvidia Docker Runtime.
   * - -gpu
     - Aliases to a specific ``-cuXX`` tagged image.
   * - <no tag>
     - Aliases to ``-cpu`` tagged images.

Example: for the nightly image based on ``Python 3.9`` and without GPU support, the tag is ``nightly-py39-cpu``.

If you want to tweak some aspects of these images and build them locally, refer to the following script:

.. code-block:: bash

  cd ray
  ./build-docker.sh

Review images by listing them:

.. code-block:: bash

  docker images

Output should look something like the following:

.. code-block:: bash

  REPOSITORY                          TAG                 IMAGE ID            CREATED             SIZE
  rayproject/ray                      dev                 7243a11ac068        2 days ago          1.11 GB
  rayproject/base-deps                latest              5606591eeab9        8 days ago          512  MB
  ubuntu                              22.04               1e4467b07108        3 weeks ago         73.9 MB


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


Installed Python dependencies
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Our docker images are shipped with pre-installed Python dependencies
required for Ray and its libraries.

We publish the dependencies that are installed in our ``ray`` Docker images for Python 3.9.

.. tab-set::

    .. tab-item:: ray (Python 3.9)
        :sync: ray (Python 3.9)

        Ray version: nightly (`18b2d94 <https://github.com/ray-project/ray/commit/18b2d948a54ae3f6312b7d2d231a8e72e8d3f353>`_)

        .. literalinclude:: ./pip_freeze_ray-py39-cpu.txt

.. _ray-install-java:

Install Ray Java with Maven
---------------------------

.. note::
   
   All Ray Java APIs are experimental and only supported by the community. 

Before installing Ray Java with Maven, you should install Ray Python with `pip install -U ray` . Note that the versions of Ray Java and Ray Python must match.
Note that nightly Ray python wheels are also required if you want to install Ray Java snapshot version.

Find the latest Ray Java release in the `central repository <https://mvnrepository.com/artifact/io.ray>`__. To use the latest Ray Java release in your application, add the following entries in your ``pom.xml``:

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

.. _ray-install-cpp:

Install Ray C++
---------------

.. note::

  All Ray C++ APIs are experimental and only supported by the community. 

You can install and use Ray C++ API as follows.

.. code-block:: bash

  pip install -U ray[cpp]

  # Create a Ray C++ project template to start with.
  ray cpp --generate-bazel-project-template-to ray-template

.. note::

  If you build Ray from source, remove the build option ``build --cxxopt="-D_GLIBCXX_USE_CXX11_ABI=0"`` from the file ``cpp/example/.bazelrc`` before running your application. The related issue is `this <https://github.com/ray-project/ray/issues/26031>`_.
