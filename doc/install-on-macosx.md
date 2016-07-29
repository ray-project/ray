# Installation on Mac OS X

Ray must currently be built from source.

## Clone the Ray repository

```
git clone https://github.com/amplab/ray.git
```

## Dependencies

First install the dependencies using brew. We currently do not support Python 3.
If you have trouble installing the Python packages, you may find it easier to
install [Anaconda](https://www.continuum.io/downloads).

```
brew update
brew install git cmake automake autoconf libtool boost graphviz
sudo easy_install pip
sudo pip install ipython --user
sudo pip install numpy typing funcsigs subprocess32 protobuf==3.0.0a2 colorama graphviz cloudpickle --ignore-installed six
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
