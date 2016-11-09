# Installation on Mac OS X

Ray must currently be built from source. We have tested Ray on OS X 10.11.

## Clone the Ray repository

```
git clone https://github.com/ray-project/ray.git
```

## Dependencies

First install the dependencies using brew. We currently do not support Python 3.
If you have trouble installing the Python packages, you may find it easier to
install [Anaconda](https://www.continuum.io/downloads).

```
brew update
brew install git cmake automake autoconf libtool boost
sudo easy_install pip
sudo pip install numpy funcsigs colorama --ignore-installed six
sudo pip install --upgrade git+git://github.com/cloudpipe/cloudpickle.git@0d225a4695f1f65ae1cbb2e0bbc145e10167cce4  # We use the latest version of cloudpickle because it can serialize named tuples.
```

## Build

Then run the setup scripts.

```
cd ray
./setup.sh # Build all necessary third party libraries (e.g., gRPC and Apache Arrow). This may take about 10 minutes.
./build.sh # Build Ray.
source setup-env.sh # Add Ray to your Python path.
```

For convenience, you may also want to add the line `source
"$RAY_ROOT/setup-env.sh"` to the bottom of your `~/.bashrc` file manually, where
`$RAY_ROOT` is the Ray directory (e.g., `/home/ubuntu/ray`).

## Test if the installation succeeded

To test if the installation was successful, try running some tests.

```
python test/runtest.py # This tests basic functionality.
python test/array_test.py # This tests some array libraries.
```
