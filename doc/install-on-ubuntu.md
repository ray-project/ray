## Installation on Ubuntu

Ray must currently be built from source.

### Clone the Ray repository

```
git clone https://github.com/amplab/ray.git
```

### Dependencies

First install the dependencies.

```
sudo apt-get update
sudo apt-get install -y git cmake build-essential autoconf curl libtool python-dev python-numpy python-pip libboost-all-dev unzip libjpeg8-dev graphviz
sudo pip install ipython typing funcsigs subprocess32 protobuf==3.0.0-alpha-2 boto3 botocore Pillow colorama graphviz
```

### Build

Then run the setup scripts.

```
cd ray
./setup.sh # This builds all necessary third party libraries (e.g., gRPC and Apache Arrow).
./build.sh # This builds Ray.
source setup-env.sh # This adds Ray to your Python path.
```

For convenience, you may also want to add the line `source
"$RAY_ROOT/setup-env.sh"` to your `~/.bashrc` file manually, where `$RAY_ROOT`
is the Ray directory (e.g., `/home/ubuntu/ray`).

### Test if the installation succeeded

To test if the installation was successful, try running some tests.

```
python test/runtest.py # This tests basic functionality.
python test/array_test.py # This tests some array libraries.
```
