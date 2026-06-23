---
myst:
  html_meta:
    description: "Step-by-step instructions for building Ray from source, including Python-only fast-loop development, full C++ editable installs, manylinux wheels, and Docker images. Use this to set up a development environment for making and testing changes to Ray."
---

(building-ray)=

# Building Ray from source

To contribute to the Ray repository, follow these instructions to build from the latest master branch.

Depending on your goal, you may not need all sections on this page:

- **Python-only development (fast loop, no C++)**: edit Python files without compiling C++ (see {ref}`python-develop`).
- **Build Ray with C++**: choose one:

  - **Distributable manylinux wheel**: uses a manylinux build container to produce a `.whl` file for installation on a cluster, for testing the packaged artifact locally, or for sharing (see {ref}`build-distributable-wheel`).
  - **Ray image**: build a nightly-style `rayproject/ray` or `rayproject/ray-llm` image (see {ref}`build-ray-image`).
  - **Full source build (editable install)**: make C++ changes or build all of Ray (see {ref}`full-source-build`).

```{contents}
:local:
:backlinks: none
```

## Setup

(fork-ray-repo)=

### Fork the Ray repository

Forking an open source repository is a best practice when contributing. You can make and test changes without affecting the original project, which keeps collaboration clean and organized. You can propose changes by submitting a pull request to the main project's repository.

1. Navigate to the [Ray GitHub repository](https://github.com/ray-project/ray).
2. Follow these [GitHub instructions](https://docs.github.com/en/get-started/quickstart/fork-a-repo), and do the following:

   1. [Fork the repo](https://docs.github.com/en/get-started/quickstart/fork-a-repo#forking-a-repository) using your preferred method.
   2. [Clone](https://docs.github.com/en/get-started/quickstart/fork-a-repo#cloning-your-forked-repository) to your local machine.
   3. [Connect your repo](https://docs.github.com/en/get-started/quickstart/fork-a-repo#configuring-git-to-sync-your-fork-with-the-upstream-repository) to the upstream (main project) Ray repo to sync changes.

(prepare-venv)=

### Prepare a Python virtual environment

Skip this section if you're building a {ref}`distributable wheel <build-distributable-wheel>` or a {ref}`Ray image <build-ray-image>`.

Create a virtual environment to prevent version conflicts and to develop with an isolated, project-specific Python setup.

::::{tab-set}

:::{tab-item} conda
Set up a `conda` environment named `myenv`:

```shell
conda create -c conda-forge python=3.10 -n myenv
```

Activate your virtual environment to tell the shell or terminal to use this particular Python:

```shell
conda activate myenv
```

You need to activate the virtual environment every time you start a new shell or terminal to work on Ray.
:::

:::{tab-item} venv
Use Python's integrated `venv` module to create a virtual environment called `myenv` in the current directory:

```shell
python -m venv myenv
```

This contains a directory with all the packages used by the local Python of your project. You only need to do this step once.

Activate your virtual environment to tell the shell or terminal to use this particular Python:

```shell
source myenv/bin/activate
```

You need to activate the virtual environment every time you start a new shell or terminal to work on Ray.

Creating a new virtual environment can come with older versions of `pip` and `wheel`. To avoid problems when you install packages, use the module `pip` to install the latest version of `pip` (itself) and `wheel`:

```shell
python -m pip install --upgrade pip wheel
```
:::

::::

(python-develop)=

## Building Ray (Python only)

:::{note}
Unless otherwise stated, directory and file paths are relative to the project root directory.
:::

RLlib, Tune, Autoscaler, and most Python files don't require you to build and compile Ray. Follow these instructions to develop Ray's Python files locally without building Ray.

1. Make sure you have a clone of Ray's git repository (see {ref}`fork-ray-repo`).

2. Make sure you activate the Python (virtual) environment (see {ref}`prepare-venv`).

3. Pip install the **latest Ray wheels**. See {ref}`install-nightlies` for instructions.

```shell
# For example, for Python 3.10:
pip install -U https://s3-us-west-2.amazonaws.com/ray-wheels/latest/ray-3.0.0.dev0-cp310-cp310-manylinux2014_x86_64.whl
```

4. Replace Python files in the installed package with your local editable copy. Use the script `python python/ray/setup-dev.py` to do this. The script removes the `ray/tune`, `ray/rllib`, `ray/autoscaler` directories (among others) bundled with the `ray` pip package, replacing them with links to your local code. This way, changing files in your git clone directly affects the behavior of your installed Ray.

```shell
# This replaces `<package path>/site-packages/ray/<package>`
# with your local `ray/python/ray/<package>`.
python python/ray/setup-dev.py
```

You can optionally skip creating symbolic links for specific directories:

```shell
# This links all folders except "_private" and "dashboard" without user prompt.
python python/ray/setup-dev.py -y --skip _private dashboard
```

(python-develop-uninstall)=

:::{warning}
Don't run `pip uninstall ray` or `pip install -U` (for Ray or Ray wheels) if setting up your environment this way. To uninstall or upgrade, first `rm -rf` the pip-installation site (usually a directory at the `site-packages/ray` location), then do a pip reinstall (see the command above), and finally run the `setup-dev.py` script again.
:::

```shell
# To uninstall, delete the symlinks first.
rm -rf <package path>/site-packages/ray # Path will be in the output of `setup-dev.py`.
pip uninstall ray # or `pip install -U <wheel>`
```

(build-distributable-wheel)=

## Building distributable manylinux wheels

:::{dropdown} Setup
:open:

Before you begin, make sure you have:

- A clone of the Ray repository (see {ref}`fork-ray-repo`)
- [uv](https://docs.astral.sh/uv/) installed
- [Docker](https://docs.docker.com/get-docker/) installed
:::

To build a distributable manylinux `.whl`, use the `build-wheel.sh` script at the repository root.

```bash
# Build a manylinux wheel for the host architecture:
./build-wheel.sh 3.12

# Specify a custom output directory:
./build-wheel.sh 3.12 ./dist
```

Run `./build-wheel.sh` without arguments to see supported Python versions and options. Regardless of host OS, the output is always a manylinux wheel (the same format used by CI and PyPI). Supported build hosts are Linux x86_64, Linux aarch64, and macOS ARM64.

See `python/README-building-wheels.md` for additional options, including building manylinux wheels directly with Docker.

(build-ray-image)=

## Building Ray images

:::{dropdown} Setup
:open:

Before you begin, make sure you have:

- A clone of the Ray repository (see {ref}`fork-ray-repo`)
- [uv](https://docs.astral.sh/uv/) installed
- [Docker](https://docs.docker.com/get-docker/) installed
:::

To build a Ray image, use the `build-image.sh` script at the repository root.

```bash
# Build the default Ray image:
./build-image.sh ray

# Build with a specific Python version:
./build-image.sh ray -p 3.12

# Build a GPU image:
./build-image.sh ray --platform cu12.8.1-cudnn
```

Run `./build-image.sh --help` to see available image types, Python versions, and platform variants.

(full-source-build)=

## Full source build

:::{tip}
If you already followed the instructions in {ref}`python-develop` and want to switch to the full build, first uninstall Ray (see {ref}`uninstallation steps <python-develop-uninstall>`).
:::

### Preparing to build Ray on Linux

:::{tip}
If you're only editing Tune, RLlib, or Autoscaler files, follow {ref}`python-develop` instead to avoid long build times.
:::

To build Ray on Ubuntu, run the following commands:

```bash
sudo apt-get update
sudo apt-get install -y build-essential curl clang-12 pkg-config psmisc unzip

# Install Bazelisk.
ci/env/install-bazel.sh

# Install node version manager and node 14
curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.40.1/install.sh | bash
nvm install 14
nvm use 14
```

The `install-bazel.sh` script installs `bazelisk`. Note that `bazel` is installed at `$HOME/bin/bazel`. Make sure it's on your `PATH`. If you prefer to use `bazel` directly, only version `7.5.0` is supported.

### Preparing to build Ray on macOS

If you have grpc or protobuf installed, remove them first for a smooth build: `brew uninstall grpc`, `brew uninstall protobuf`. If the build fails with `No such file or directory` errors, clean previous builds with `brew uninstall binutils` and `bazel clean --expunge`.

To build Ray on macOS, first install these dependencies:

```bash
brew update
brew install wget

# Install Bazel.
ci/env/install-bazel.sh
```

### Building Ray on Linux and macOS (full)

Make sure you have a local clone of Ray's git repository (see {ref}`fork-ray-repo`). You also need to install [NodeJS](https://nodejs.org) to build the dashboard.

Go to the project directory, for example:

```shell
cd ray
```

Build the dashboard. From inside your local Ray project directory, go to the dashboard client directory:

```bash
cd python/ray/dashboard/client
```

Install the dependencies and build the dashboard:

```bash
npm ci
npm run build
```

Move back to the top-level Ray directory:

```shell
cd -
```

Now let's build Ray for Python. Make sure you activate any Python virtual (or conda) environment (see {ref}`prepare-venv`).

Go to the `python/` directory inside the Ray project directory and install the project with `pip`:

```bash
# Install Ray.
cd python/
# Install required dependencies.
pip install -r requirements.txt
# You may need to set the following two env vars if you have a macOS ARM64(M1) platform.
# See https://github.com/grpc/grpc/issues/25082 for more details.
# export GRPC_PYTHON_BUILD_SYSTEM_OPENSSL=1
# export GRPC_PYTHON_BUILD_SYSTEM_ZLIB=1
pip install -e . --verbose  # Add --user if you see a permission denied error.
```

The `-e` means "editable", so changes you make to files in the Ray directory take effect without reinstalling the package.

:::{warning}
Don't run `python setup.py install`. Python copies files from the Ray directory to a packages directory (`/lib/python3.6/site-packages/ray`), so changes you make to files in the Ray directory won't have any effect.
:::

If your machine runs out of memory during the build, add the following to `~/.bazelrc`:

> ```shell
> build --local_resources=memory=HOST_RAM*.5 --local_resources=cpu=4
> ```
>
> The `build --disk_cache=~/bazel-cache` option can also speed up repeated builds.

If you run into an error building protobuf, switching from miniforge to anaconda might help.

### Building Ray on Windows (full)

**Requirements**

The following links were accurate at the time of writing. If a URL has changed, search the organization's site.

- Bazel 7.5.0 (<https://github.com/bazelbuild/bazel/releases/tag/7.5.0>)
- Microsoft Visual Studio 2019 (or Microsoft Build Tools 2019 - <https://visualstudio.microsoft.com/downloads/#build-tools-for-visual-studio-2019>)
- JDK 15 (<https://www.oracle.com/java/technologies/javase-jdk15-downloads.html>)
- Miniforge 3 (<https://github.com/conda-forge/miniforge/blob/main/README.md>)
- git for Windows, version 2.31.1 or later (<https://git-scm.com/download/win>)

You can also use the included script to install Bazel:

```bash
# Install Bazel.
ray/ci/env/install-bazel.sh
# (Windows users: please manually place Bazel in your PATH, and point
# BAZEL_SH to MSYS2's Bash: ``set BAZEL_SH=C:\Program Files\Git\bin\bash.exe``)
```

**Steps**

1. Enable Developer mode on Windows 10 systems. This is necessary so git can create symlinks.

   1. Open the Settings app.
   2. Go to "Update & Security".
   3. Go to "For Developers" in the left pane.
   4. Turn on "Developer mode".

2. Add the following Miniforge subdirectories to PATH. If Miniforge was installed for all users, the following paths are correct. If Miniforge is installed for a single user, adjust the paths accordingly.

   - `C:\ProgramData\miniforge3`
   - `C:\ProgramData\miniforge3\Scripts`
   - `C:\ProgramData\miniforge3\Library\bin`

3. Define an environment variable `BAZEL_SH` to point to `bash.exe`. If git for Windows was installed for all users, bash's path should be `C:\Program Files\Git\bin\bash.exe`. If git was installed for a single user, adjust the path accordingly.

4. Install Bazel 7.5.0. Go to the Bazel 7.5.0 release page and download `bazel-7.5.0-windows-x86_64.exe`. Copy the exe into the directory of your choice. Define an environment variable `BAZEL_PATH` to the full exe path (example: `set BAZEL_PATH=C:\bazel\bazel.exe`). Also add the Bazel directory to `PATH` (example: `set PATH=%PATH%;C:\bazel`).

5. Download the Ray source code and build it.

```shell
# cd to the directory under which the ray source tree will be downloaded.
git clone -c core.symlinks=true https://github.com/ray-project/ray.git
cd ray\python
pip install -e . --verbose
```

## Advanced build options

### Environment variables that influence builds

You can tweak the build with the following environment variables (when running `pip install -e .` or `python setup.py install`):

- `RAY_BUILD_CORE`: If set and equal to `1`, Ray builds the core parts. Defaults to `1`.
- `RAY_INSTALL_JAVA`: If set and equal to `1`, Ray runs extra build steps to build Java portions of the codebase.
- `RAY_INSTALL_CPP`: If set and equal to `1`, Ray installs `ray-cpp`.
- `RAY_BUILD_REDIS`: If set and equal to `1`, Ray builds or fetches Redis binaries. These binaries are only used for testing. Defaults to `1`.
- `RAY_DISABLE_EXTRA_CPP`: If set and equal to `1`, a regular (non-`cpp`) build won't provide some `cpp` interfaces.
- `SKIP_BAZEL_BUILD`: If set and equal to `1`, Ray skips all Bazel build steps.
- `SKIP_THIRDPARTY_INSTALL_CONDA_FORGE`: If set, setup skips installation of third-party packages required for build. This is active on conda-forge where pip isn't used to create a build environment.
- `RAY_DEBUG_BUILD`: Can be set to `debug`, `asan`, or `tsan`. Ray ignores any other value.
- `BAZEL_ARGS`: If set, pass a space-separated set of arguments to Bazel. This can be useful for restricting resource usage during builds, for example. See [the Bazel user manual](https://bazel.build/docs/user-manual) for more information about valid arguments.
- `IS_AUTOMATED_BUILD`: Used in conda-forge CI to tweak the build for the managed CI machines.
- `SRC_DIR`: Can be set to the root of the source checkout, defaults to `None`, which is `cwd()`.
- `BAZEL_SH`: Used on Windows to find `bash.exe`. See below.
- `BAZEL_PATH`: Used on Windows to find `bazel.exe`. See below.
- `MINGW_DIR`: Used on Windows to find `bazel.exe` if not found in `BAZEL_PATH`.

### Fast, debug, and optimized builds

By default, Ray builds with optimizations, which can take a long time and interfere with debugging. To perform fast, debug, or optimized builds, run the following (via `-c` with `fastbuild`, `dbg`, or `opt`, respectively):

```shell
bazel run -c fastbuild //:gen_ray_pkg
```

This rebuilds Ray with the appropriate options (which might take a while). If you need to build all targets, use `bazel build //:all` instead of `bazel run //:gen_ray_pkg`.

To make this change permanent, you can add an option such as the following line to your user-level `~/.bazelrc` file (not to be confused with the workspace-level `.bazelrc` file):

```shell
build --compilation_mode=fastbuild
```

If you do so, remember to revert this change, unless you want it to affect all of your development in the future.

Using `dbg` instead of `fastbuild` generates more debug information, which can make it easier to debug with a debugger such as `gdb`.

### Using a local repository for dependencies

If you'd like to build Ray with custom dependencies (for example, with a different version of Cython), you can modify your `.bzl` file as follows:

```python
http_archive(
  name = "cython",
  ...,
) if False else native.new_local_repository(
  name = "cython",
  build_file = "bazel/BUILD.cython",
  path = "../cython",
)
```

This replaces the existing `http_archive` rule with one that references a sibling of your Ray directory (named `cython`) using the build file provided in the Ray repository (`bazel/BUILD.cython`). If the dependency already has a Bazel build file in it, you can use `native.local_repository` instead, and omit `build_file`.

To test switching back to the original rule, change `False` to `True`.

## Development tooling

### Installing additional dependencies for development

Install dependencies for the linter (`pre-commit`):

```shell
pip install -c python/requirements_compiled.txt pre-commit
pre-commit install
```

Install dependencies for running Ray unit tests under `python/ray/tests`:

```shell
pip install -c python/requirements_compiled.txt -r python/requirements/test-requirements.txt
```

Requirement files for running Ray Data and ML library tests are under `python/requirements/`.

### Pre-commit hooks

Ray uses pre-commit hooks with [the pre-commit Python package](https://pre-commit.com/). The `.pre-commit-config.yaml` file configures all the linting and formatting checks. To start using `pre-commit`:

```shell
pip install pre-commit
pre-commit install
```

This installs pre-commit into the current environment and enables pre-commit checks every time you commit new code changes with git. To temporarily skip pre-commit checks, use the `-n` or `--no-verify` flag when committing:

```shell
git commit -n
```

If you encounter any issues with `pre-commit`, [report an issue](https://github.com/ray-project/ray/issues/new?template=bug-report.yml).

## Building the docs

To learn more about building the docs, see [Contributing to the Ray documentation](https://docs.ray.io/en/master/ray-contribute/docs.html).

## Troubleshooting

If importing Ray (`python3 -c "import ray"`) in your development clone results in this error:

```python
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
```

Run the following commands:

```bash
rm -rf python/ray/thirdparty_files/
python3 -m pip install psutil
```
