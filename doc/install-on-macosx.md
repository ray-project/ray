## Installation on Mac OS X

Ray must currently be built from source.

### Clone the Ray repository

```
git clone https://github.com/amplab/ray.git
```

### Dependencies

First install the dependencies using brew.

```
brew update
brew install git cmake automake autoconf libtool boost libjpeg graphviz
sudo easy_install pip
sudo pip install ipython --user
sudo pip install numpy typing funcsigs subprocess32 protobuf==3.0.0-alpha-2 boto3 botocore Pillow colorama graphviz --ignore-installed six
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
