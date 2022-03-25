.. _building-ray:

Building Ray from Source
=========================

For a majority of Ray users, installing Ray via the latest wheels or pip package is usually enough. However, you may want to build the latest master branch.

.. tip:: If you are only editing Python files, follow instructions for :ref:`python-develop` to avoid long build times.

.. contents::
  :local:

.. _python-develop:

Building Ray (Python Only)
--------------------------

.. note:: Unless otherwise stated, directory and file paths are relative to the project root directory.

RLlib, Tune, Autoscaler, and most Python files do not require you to build and compile Ray. Follow these instructions to develop Ray's Python files locally without building Ray.

0. (Optional) To setup an isolated Anaconda environment, see :ref:`ray_anaconda`.

1. Pip install the **latest Ray wheels.** See :ref:`install-nightlies` for instructions.

.. code-block:: shell

    pip install -U https://s3-us-west-2.amazonaws.com/ray-wheels/latest/ray-2.0.0.dev0-cp38-cp38-manylinux2014_x86_64.whl

2. Fork and clone the project to your machine. Connect your repository to the upstream (main project) ray repository.

.. code-block:: shell

    git clone https://github.com/[your username]/ray.git
    cd ray
    git remote add upstream https://github.com/ray-project/ray.git
    # Make sure you are up-to-date on master.

3. Replace Python files in the installed package with your local editable copy. We provide a simple script to help you do this: ``python python/ray/setup-dev.py``.
Running the script will remove the  ``ray/tune``, ``ray/rllib``, ``ray/autoscaler`` dir (among other directories) bundled with the ``ray`` pip package, and replace them with links to your local code. This way, changing files in your git clone will directly affect the behavior of your installed ray.

.. code-block:: shell

    # This replaces `<package path>/site-packages/ray/<package>`
    # with your local `ray/python/ray/<package>`.
    python python/ray/setup-dev.py

.. warning:: Do not run ``pip uninstall ray`` or ``pip install -U`` (for Ray or Ray wheels) if setting up your environment this way. To uninstall or upgrade, you must first ``rm -rf`` the pip-installation site (usually a ``site-packages/ray`` location), then do a pip reinstall (see 1. above), and finally run the above `setup-dev.py` script again.

.. code-block:: shell

    # To uninstall, delete the symlinks first.
    rm -rf <package path>/site-packages/ray # Path will be in the output of `setup-dev.py`.
    pip uninstall ray # or `pip install -U <wheel>`

Building Ray on Linux & MacOS (full)
------------------------------------

.. tip:: If you are only editing Tune/RLlib/Autoscaler files, follow instructions for :ref:`python-develop` to avoid long build times.

To build Ray, first install the following dependencies.

For Ubuntu, run the following commands:

.. code-block:: bash

  sudo apt-get update
  sudo apt-get install -y build-essential curl unzip psmisc

For RHELv8 (Redhat EL 8.0-64 Minimal), run the following commands:

.. code-block:: bash

  sudo yum groupinstall 'Development Tools'
  sudo yum install psmisc

Install bazel manually from link: https://docs.bazel.build/versions/main/install-redhat.html 


For MacOS, run the following commands:

.. tip:: Assuming you already have brew and bazel installed on your mac and you also have grpc and protobuf installed on your mac consider removing those (grpc and protobuf) for smooth build through commands ``brew uninstall grpc``, ``brew uninstall protobuf``. If you have built the source code earlier and it still fails with error as ``No such file or directory:``, try cleaning previous builds on your host by running commands ``brew uninstall binutils`` and ``bazel clean --expunge``.


.. code-block:: bash

  brew update
  brew install wget

Ray can be built from the repository as follows.

.. code-block:: bash

  git clone https://github.com/ray-project/ray.git

  # Install Bazel.
  ray/ci/travis/install-bazel.sh
  # (Windows users: please manually place Bazel in your PATH, and point
  # BAZEL_SH to MSYS2's Bash: ``set BAZEL_SH=C:\Program Files\Git\bin\bash.exe``)

  # Build the dashboard
  # (requires Node.js, see https://nodejs.org/ for more information).
  pushd ray/dashboard/client
  npm install
  npm run build
  popd

  # Install Ray.
  cd ray/python
  pip install -e . --verbose  # Add --user if you see a permission denied error.

The ``-e`` means "editable", so changes you make to files in the Ray
directory will take effect without reinstalling the package.

.. warning:: if you run ``python setup.py install``, files will be copied from the Ray directory to a directory of Python packages (``/lib/python3.6/site-packages/ray``). This means that changes you make to files in the Ray directory will not have any effect.

.. tip:: 

  If your machine is running out of memory during the build or the build is causing other programs to crash, try adding the following line to ``~/.bazelrc``:

  ``build --local_ram_resources=HOST_RAM*.5 --local_cpu_resources=4``

  The ``build --disk_cache=~/bazel-cache`` option can be useful to speed up repeated builds too.

Building Ray on Windows (full)
------------------------------

**Requirements**

The following links were correct during the writing of this section. In case the URLs changed, search at the organizations' sites.

- bazel 4.2 (https://github.com/bazelbuild/bazel/releases/tag/4.2.1)
- Microsoft Visual Studio 2019 (or Microsoft Build Tools 2019 - https://visualstudio.microsoft.com/downloads/#build-tools-for-visual-studio-2019)
- JDK 15 (https://www.oracle.com/java/technologies/javase-jdk15-downloads.html)
- Miniconda 3 (https://docs.conda.io/en/latest/miniconda.html)
- git for Windows, version 2.31.1 or later (https://git-scm.com/download/win)

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

3. Define an environment variable BAZEL_SH to point to bash.exe. If git for Windows was installed for all users, bash's path should be ``C:\Program Files\Git\bin\bash.exe``. If git was installed for a single user, adjust the path accordingly.

4. Bazel 4.2 installation. Go to bazel 4.2 release web page and download
bazel-4.2.1-windows-x86_64.exe. Copy the exe into the directory of your choice.
Define an environment variable BAZEL_PATH to full exe path (example:
``set BAZEL_PATH=C:\bazel\bazel.exe``). Also add the bazel directory to the
``PATH`` (example: ``set PATH=%PATH%;C:\bazel``)

5. Download ray source code and build it.

.. code-block:: shell

  # cd to the directory under which the ray source tree will be downloaded.
  git clone -c core.symlinks=true https://github.com/ray-project/ray.git
  cd ray\python
  pip install -e . --verbose

Environment variables that influence builds
--------------------------------------------

You can tweak the build with the following environment variables (when running ``setup.py``):

- ``BUILD_JAVA``: If set and equal to ``1``, extra build steps will be executed
  to build java portions of the codebase
- ``RAY_INSTALL_CPP``: If set and equal to ``1``, ``ray-cpp`` will be installed
- ``RAY_DISABLE_EXTRA_CPP``: If set and equal to ``1``, a regular (non -
  ``cpp``) build will not provide some ``cpp`` interfaces
- ``SKIP_BAZEL_BUILD``: If set and equal to ``1``, no bazel build steps will be
  executed
- ``SKIP_THIRDPARTY_INSTALL``: If set will skip installation of third-party
  python packages
- ``RAY_DEBUG_BUILD``: Can be set to ``debug``, ``asan``, or ``tsan``. Any
  other value will be ignored
- ``BAZEL_LIMIT_CPUS``: If set, it must be an integers. This will be fed to the
  ``--local_cpu_resources`` argument for the call to bazel, which will limit the
  number of CPUs used during bazel steps.
- ``IS_AUTOMATED_BUILD``: Used in CI to tweak the build for the CI machines
- ``SRC_DIR``: Can be set to the root of the source checkout, defaults to
  ``None`` which is ``cwd()``
- ``BAZEL_SH``: used on Windows to find a ``bash.exe``, see below
- ``BAZEL_PATH``: used on Windows to find ``bazel.exe``, see below
- ``MINGW_DIR``: used on Windows to find ``bazel.exe`` if not found in ``BAZEL_PATH``

Installing additional dependencies for development
--------------------------------------------------

Dependencies for the linter (``scripts/format.h``) can be installed with:

.. code-block:: shell

 pip install -r python/requirements_linters.txt

Dependencies for running Ray unit tests under ``python/ray/tests`` can be installed with:

.. code-block:: shell

 pip install -r python/requirements.txt

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

If you make changes that require documentation changes, don't forget to
update the documentation!

When you make documentation changes, build them locally to verify they render
correctly. `Sphinx <http://sphinx-doc.org/>`_ is used to generate the documentation.

.. code-block:: shell

    cd doc
    pip install -r requirements-doc.txt
    make html

Once done, the docs will be in ``doc/_build/html``. For example, on Mac
OSX, you can open the docs (assuming you are still in the ``doc``
directory) using ``open _build/html/index.html``.


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
