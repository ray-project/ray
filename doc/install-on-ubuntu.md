# Installation on Ubuntu

Ray should work with Python 2 and Python 3. We have tested Ray on Ubuntu 14.04
and Ubuntu 16.04

## Dependencies

To install Ray, first install the following dependencies. We recommend using
[Anaconda](https://www.continuum.io/downloads).

```
sudo apt-get update
sudo apt-get install -y cmake build-essential autoconf curl libtool python-dev python-pip libboost-all-dev unzip nodejs npm  # If you're using Anaconda, then python-dev and python-pip are unnecessary.

pip install numpy funcsigs colorama psutil redis
pip install --upgrade git+git://github.com/cloudpipe/cloudpickle.git@0d225a4695f1f65ae1cbb2e0bbc145e10167cce4  # We use the latest version of cloudpickle because it can serialize named tuples.
pip install --upgrade --verbose "git+git://github.com/ray-project/ray.git#egg=numbuf&subdirectory=numbuf"
```

# Install Ray

Ray can be built from the repository as follows.

```
git clone https://github.com/ray-project/ray.git
cd lib/python
python setup.py install
```

Alternatively, Ray can be installed with pip as follows. However, this is
slightly less likely to succeed.

```
pip install --upgrade --verbose "git+git://github.com/ray-project/ray.git#egg=ray&subdirectory=lib/python"
```

## Test if the installation succeeded

To test if the installation was successful, try running some tests. This assumes
that you've cloned the git repository.

```
python test/runtest.py
```
