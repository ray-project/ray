.. _building-ray:

Building Ray from Source
=========================

For a majority of Ray users, installing Ray via the latest wheels or pip package is usually enough. However, you may want to build the latest master branch.

.. tip::

  If you are only editing Python files, follow instructions for :ref:`python-develop` to avoid long build times.

  If you already followed the instructions in :ref:`python-develop` and want to switch to the Full build in this section, you will need to first uninstall.

.. contents::
  :local:

Clone the repository
--------------------

To build Ray locally you will need to have the Git repository, so first, fork it on GitHub. Then you can clone it to your machine:

.. tabbed:: Git SSH

    To clone the repository using Git with SSH (the default) run:

    .. code-block:: shell

        git clone git@github.com:[your username]/ray.git

.. tabbed:: Git HTTPS

    To clone the repository using Git with HTTPS run:

    .. code-block:: shell

        git clone https://github.com/[your username]/ray.git

Then you can enter into the Ray git repository directory:

.. code-block:: shell

    cd ray

Next make sure you connect your repository to the upstream (main project) Ray repository. This will allow you to push your code to your repository when proposing changes (in pull requests) while also pulling updates from the main project.

.. tabbed:: Git SSH

    To connect your repository using SSH (the default) run the command:

    .. code-block:: shell

        git remote add upstream git@github.com:ray-project/ray.git

.. tabbed:: Git HTTPS

    To connect your repository using HTTPS run the command:

    .. code-block:: shell

        git remote add upstream https://github.com/ray-project/ray.git

Every time you want to update your local version you can pull the changes from the main repository:

.. code-block:: shell

    # Checkout the local master branch
    git checkout master
    # Pull the latest changes from the main repository
    git pull upstream master

Prepare the Python environment
------------------------------

You probably want some type of Python virtual environment. For example, you can use Anaconda's ``conda``. 

.. tabbed:: conda

    Set up a ``conda`` environment named ``ray``:

    .. code-block:: shell

        conda create -c conda-forge python=3.9 -n ray


    Activate your virtual environment to tell the shell/terminal to use this particular Python:

    .. code-block:: shell
		    
        conda activate ray
        
    You need to activate the virtual environment every time you start a new shell/terminal to work on Ray.

.. tabbed:: venv

    Use Python's integrated ``venv`` module to create a virtual environment called ``venv`` in the current directory:

    .. code-block:: shell

        python -m venv venv

    This contains a directory with all the packages used by the local Python of your project. You only need to do this step once.

    Activate your virtual environment to tell the  shell/terminal to use this particular Python:

    .. code-block:: shell

        source venv/bin/activate

    You need to activate the virtual environment every time you start a new shell/terminal to work on Ray.

    Creating a new virtual environment can come with older versions of ``pip`` and ``wheel``. To avoid problems when you install packages, use the module ``pip`` to install the latest version of ``pip`` (itself) and ``wheel``:

    .. code-block:: shell

        python -m pip install --upgrade pip wheel

.. _python-develop:

Building Ray (Python Only)
--------------------------

.. note:: Unless otherwise stated, directory and file paths are relative to the project root directory.

RLlib, Tune, Autoscaler, and most Python files do not require you to build and compile Ray. Follow these instructions to develop Ray's Python files locally without building Ray.

1. Make sure you have a clone of Ray's git repository as explained above.

2. Make sure you activate the Python (virtual) environment as described above.

3. Pip install the **latest Ray wheels.** See :ref:`install-nightlies` for instructions.

.. code-block:: shell

    # For example, for Python 3.8:
    pip install -U https://s3-us-west-2.amazonaws.com/ray-wheels/latest/ray-3.0.0.dev0-cp38-cp38-manylinux2014_x86_64.whl

4. Replace Python files in the installed package with your local editable copy. We provide a simple script to help you do this: ``python python/ray/setup-dev.py``. Running the script will remove the  ``ray/tune``, ``ray/rllib``, ``ray/autoscaler`` dir (among other directories) bundled with the ``ray`` pip package, and replace them with links to your local code. This way, changing files in your git clone will directly affect the behavior of your installed Ray.

.. code-block:: shell

    # This replaces `<package path>/site-packages/ray/<package>`
    # with your local `ray/python/ray/<package>`.
    python python/ray/setup-dev.py

.. note:: [Advanced] You can also optionally skip creating symbolic link for directories of your choice.

.. code-block:: shell

    # This links all folders except "_private" and "dashboard" without user prompt.
    python setup-dev.py -y --skip _private dashboard

.. warning:: Do not run ``pip uninstall ray`` or ``pip install -U`` (for Ray or Ray wheels) if setting up your environment this way. To uninstall or upgrade, you must first ``rm -rf`` the pip-installation site (usually a directory at the ``site-packages/ray`` location), then do a pip reinstall (see the command above), and finally run the above ``setup-dev.py`` script again.

.. code-block:: shell

    # To uninstall, delete the symlinks first.
    rm -rf <package path>/site-packages/ray # Path will be in the output of `setup-dev.py`.
    pip uninstall ray # or `pip install -U <wheel>`

Preparing to build Ray on Linux
-------------------------------

.. tip:: If you are only editing Tune/RLlib/Autoscaler files, follow instructions for :ref:`python-develop` to avoid long build times.

To build Ray on Ubuntu, run the following commands:

.. code-block:: bash

  # Add a PPA containing gcc-9 for older versions of Ubuntu.
  sudo add-apt-repository -y ppa:ubuntu-toolchain-r/test
  sudo apt-get update
  sudo apt-get install -y build-essential curl gcc-9 g++-9 pkg-config psmisc unzip
  sudo update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-9 90 \
                --slave /usr/bin/g++ g++ /usr/bin/g++-9 \
                --slave /usr/bin/gcov gcov /usr/bin/gcov-9

  # Install Bazel.
  ci/env/install-bazel.sh

  # Install node version manager and node 14
  $(curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.39.0/install.sh)
  nvm install 14
  nvm use 14


For RHELv8 (Redhat EL 8.0-64 Minimal), run the following commands:

.. code-block:: bash

  sudo yum groupinstall 'Development Tools'
  sudo yum install psmisc

In RedHat, install Bazel manually from this link: https://docs.bazel.build/versions/main/install-redhat.html

Preparing to build Ray on MacOS
-------------------------------

.. tip:: Assuming you already have Brew and Bazel installed on your mac and you also have grpc and protobuf installed on your mac consider removing those (grpc and protobuf) for smooth build through the commands ``brew uninstall grpc``, ``brew uninstall protobuf``. If you have built the source code earlier and it still fails with errors like ``No such file or directory:``, try cleaning previous builds on your host by running the commands ``brew uninstall binutils`` and ``bazel clean --expunge``.

To build Ray on MacOS, first install these dependencies:

.. code-block:: bash

  brew update
  brew install wget

  # Install Bazel.
  ray/ci/env/install-bazel.sh

Building Ray on Linux & MacOS (full)
------------------------------------

Make sure you have a local clone of Ray's git repository as explained above. You will also need to install NodeJS_ to build the dashboard.

Enter into the project directory, for example:

.. code-block:: shell

    cd ray

Now you can build the dashboard. From inside of your local Ray project directory enter into the dashboard client directory:

.. code-block:: bash

  cd dashboard/client

Then you can install the dependencies and build the dashboard:

.. code-block:: bash

  npm ci
  npm run build

After that, you can now move back to the top level Ray directory:

.. code-block:: shell

  cd ../..


Now let's build Ray for Python. Make sure you activate any Python virtual (or conda) environment you could be using as described above.

Enter into the ``python/`` directory inside of the Ray project directory and install the project with ``pip``:

.. code-block:: bash

  # Install Ray.
  cd python/
  # You may need to set the following two env vars if your platform is MacOS ARM64(M1).
  # See https://github.com/grpc/grpc/issues/25082 for more details.
  # export GRPC_PYTHON_BUILD_SYSTEM_OPENSSL=1
  # export GRPC_PYTHON_BUILD_SYSTEM_ZLIB=1
  pip install -e . --verbose  # Add --user if you see a permission denied error.

The ``-e`` means "editable", so changes you make to files in the Ray
directory will take effect without reinstalling the package.

.. warning:: if you run ``python setup.py install``, files will be copied from the Ray directory to a directory of Python packages (``/lib/python3.6/site-packages/ray``). This means that changes you make to files in the Ray directory will not have any effect.

.. tip::

  If your machine is running out of memory during the build or the build is causing other programs to crash, try adding the following line to ``~/.bazelrc``:

  ``build --local_ram_resources=HOST_RAM*.5 --local_cpu_resources=4``

  The ``build --disk_cache=~/bazel-cache`` option can be useful to speed up repeated builds too.

.. _NodeJS: https://nodejs.org

Building Ray on Windows (full)
------------------------------

**Requirements**

The following links were correct during the writing of this section. In case the URLs changed, search at the organizations' sites.

- Bazel 4.2 (https://github.com/bazelbuild/bazel/releases/tag/4.2.1)
- Microsoft Visual Studio 2019 (or Microsoft Build Tools 2019 - https://visualstudio.microsoft.com/downloads/#build-tools-for-visual-studio-2019)
- JDK 15 (https://www.oracle.com/java/technologies/javase-jdk15-downloads.html)
- Miniconda 3 (https://docs.conda.io/en/latest/miniconda.html)
- git for Windows, version 2.31.1 or later (https://git-scm.com/download/win)

You can also use the included script to install Bazel:

.. code-block:: bash

  # Install Bazel.
  ray/ci/env/install-bazel.sh
  # (Windows users: please manually place Bazel in your PATH, and point
  # BAZEL_SH to MSYS2's Bash: ``set BAZEL_SH=C:\Program Files\Git\bin\bash.exe``)

**Steps**

1. Enable Developer mode on Windows 10 systems. This is necessary so git can create symlinks.

   1. Open Settings app;
   2. Go to "Update & Security";
   3. Go to "For Developers" on the left pane;
   4. Turn on "Developer mode".

2. Add the following Miniconda subdirectories to PATH. If Miniconda was installed for all users, the following paths are correct. If Miniconda is installed for a single user, adjust the paths accordingly.

   - ``C:\ProgramData\Miniconda3``
   - ``C:\ProgramData\Miniconda3\Scripts``
   - ``C:\ProgramData\Miniconda3\Library\bin``

3. Define an environment variable ``BAZEL_SH`` to point to ``bash.exe``. If git for Windows was installed for all users, bash's path should be ``C:\Program Files\Git\bin\bash.exe``. If git was installed for a single user, adjust the path accordingly.

4. Bazel 4.2 installation. Go to Bazel 4.2 release web page and download
bazel-4.2.1-windows-x86_64.exe. Copy the exe into the directory of your choice.
Define an environment variable BAZEL_PATH to full exe path (example:
``set BAZEL_PATH=C:\bazel\bazel.exe``). Also add the Bazel directory to the
``PATH`` (example: ``set PATH=%PATH%;C:\bazel``)

5. Download ray source code and build it.

.. code-block:: shell

  # cd to the directory under which the ray source tree will be downloaded.
  git clone -c core.symlinks=true https://github.com/ray-project/ray.git
  cd ray\python
  pip install -e . --verbose

Environment variables that influence builds
--------------------------------------------

You can tweak the build with the following environment variables (when running ``pip install -e .`` or ``python setup.py install``):

- ``RAY_INSTALL_JAVA``: If set and equal to ``1``, extra build steps will be executed
  to build java portions of the codebase
- ``RAY_INSTALL_CPP``: If set and equal to ``1``, ``ray-cpp`` will be installed
- ``RAY_DISABLE_EXTRA_CPP``: If set and equal to ``1``, a regular (non -
  ``cpp``) build will not provide some ``cpp`` interfaces
- ``SKIP_BAZEL_BUILD``: If set and equal to ``1``, no Bazel build steps will be
  executed
- ``SKIP_THIRDPARTY_INSTALL``: If set will skip installation of third-party
  python packages
- ``RAY_DEBUG_BUILD``: Can be set to ``debug``, ``asan``, or ``tsan``. Any
  other value will be ignored
- ``BAZEL_LIMIT_CPUS``: If set, it must be an integers. This will be fed to the
  ``--local_cpu_resources`` argument for the call to Bazel, which will limit the
  number of CPUs used during Bazel steps.
- ``IS_AUTOMATED_BUILD``: Used in CI to tweak the build for the CI machines
- ``SRC_DIR``: Can be set to the root of the source checkout, defaults to
  ``None`` which is ``cwd()``
- ``BAZEL_SH``: used on Windows to find a ``bash.exe``, see below
- ``BAZEL_PATH``: used on Windows to find ``bazel.exe``, see below
- ``MINGW_DIR``: used on Windows to find ``bazel.exe`` if not found in ``BAZEL_PATH``

Installing additional dependencies for development
--------------------------------------------------

Dependencies for the linter (``scripts/format.sh``) can be installed with:

.. code-block:: shell

 pip install -r python/requirements_linters.txt

Dependencies for running Ray unit tests under ``python/ray/tests`` can be installed with:

.. code-block:: shell

 pip install -c python/requirements.txt -r python/requirements_test.txt

Requirement files for running Ray Data / ML library tests are under ``python/requirements/``.

Fast, Debug, and Optimized Builds
---------------------------------

Currently, Ray is built with optimizations, which can take a long time and
interfere with debugging. To perform fast, debug, or optimized builds, you can
run the following (via ``-c`` ``fastbuild``/``dbg``/``opt``, respectively):

.. code-block:: shell

 bazel build -c fastbuild //:ray_pkg

This will rebuild Ray with the appropriate options (which may take a while).
If you need to build all targets, you can use ``"//:*"`` instead of
``//:ray_pkg``.

To make this change permanent, you can add an option such as the following
line to your user-level ``~/.bazelrc`` file (not to be confused with the
workspace-level ``.bazelrc`` file):

.. code-block:: shell

 build --compilation_mode=fastbuild

If you do so, remember to revert this change, unless you want it to affect
all of your development in the future.

Using ``dbg`` instead of ``fastbuild`` generates more debug information,
which can make it easier to debug with a debugger like ``gdb``.

Building the Docs
-----------------

To learn more about building the docs refer to `Contributing to the Ray Documentation`_.

.. _Contributing to the Ray Documentation: https://docs.ray.io/en/master/ray-contribute/docs.html

Using a local repository for dependencies
-----------------------------------------

If you'd like to build Ray with custom dependencies (for example, with a
different version of Cython), you can modify your ``.bzl`` file as follows:

.. code-block:: python

  http_archive(
    name = "cython",
    ...,
  ) if False else native.new_local_repository(
    name = "cython",
    build_file = "bazel/BUILD.cython",
    path = "../cython",
  )

This replaces the existing ``http_archive`` rule with one that references a
sibling of your Ray directory (named ``cython``) using the build file
provided in the Ray repository (``bazel/BUILD.cython``).
If the dependency already has a Bazel build file in it, you can use
``native.local_repository`` instead, and omit ``build_file``.

To test switching back to the original rule, change ``False`` to ``True``.

.. _`PR template`: https://github.com/ray-project/ray/blob/master/.github/PULL_REQUEST_TEMPLATE.md

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
