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

.. warning:: Do not run ``pip uninstall ray`` or ``pip install -U`` (for Ray or Ray wheels) if setting up your environment this way. To uninstall or upgrade, you must first ``rm -rf`` the pip-installation site (usually a ``site-packages/ray`` location), then do a pip reinstall (see 1. above), and finally run the above `setup-dev.py` script again.

.. code-block:: shell

    cd ray
    python python/ray/setup-dev.py
    # This replaces miniconda3/lib/python3.7/site-packages/ray/tune
    # with your local `ray/python/ray/tune`.

Building Ray (full)
-------------------

.. tip:: If you are only editing Tune/RLlib/Autoscaler files, follow instructions for :ref:`python-develop` to avoid long build times.

To build Ray, first install the following dependencies.

For Ubuntu, run the following commands:

.. code-block:: bash

  sudo apt-get update
  sudo apt-get install -y build-essential curl unzip psmisc

  pip install cython==0.29.0 pytest

For MacOS, run the following commands:

.. code-block:: bash

  brew update
  brew install wget

  pip install cython==0.29.0 pytest

For Windows, see the :ref:`Windows Dependencies <windows-dependencies>` section.

Ray can be built from the repository as follows.

.. code-block:: bash

  git clone https://github.com/ray-project/ray.git

  # Install Bazel.
  # (Windows users: please manually place Bazel in your PATH, and point BAZEL_SH to MSYS2's Bash.)
  ray/ci/travis/install-bazel.sh

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


Fast, Debug, and Optimized Builds
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

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
    pip install -U -r requirements-rtd.txt # important for reproducing the deployment environment
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
