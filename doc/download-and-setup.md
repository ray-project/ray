## Download and Setup

Ray must currently be built from source.

### Clone the Ray repository

```
git clone https://github.com/amplab/ray.git
```
These instructions will install the latest master branch for Ray.

### Installation for Ubuntu and Mac OS X

For convenience, we provide a setup script that pulls the necessary
dependencies.

```
cd ray
./setup.sh # This builds all necessary third party libraries (e.g., gRPC and Apache Arrow). It will require a sudo password.
./build.sh # This builds Ray.
source setup-env.sh # This adds Ray to your Python path.
```

For convenience, you may also want to add the line `source
"$RAY_ROOT/setup-env.sh"` to your `~/.bashrc` file manually, where `$RAY_ROOT`
is the Ray directory (e.g., `/home/ubuntu/ray`).

To test if the installation was successful, try running some tests.

### Installation for Windows

Ray currently does not run on Windows. However, it can be compiled with the
following instructions.

**Note:** A batch file is provided that clones any missing third-party libraries
and applies patches to them. Do not attempt to open the solution before the
batch file applies the patches; otherwise, if the projects have been modified,
the patches may be rejected, and you may be forced to revert your changes before
re-running the batch file.

1. Install Microsoft Visual Studio 2015
2. Install Git
3. `git clone https://github.com/amplab/ray.git`
4. `ray\thirdparty\download_thirdparty.bat`

### Test if the installation succeeded

Try running some tests.

```
python test/runtest.py # This tests basic functionality.
python test/array_test.py # This tests some array libraries.
```
