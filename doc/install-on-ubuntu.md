# Installation on Ubuntu

Ray should work with Python 2 and Python 3. We have tested Ray on Ubuntu 14.04
and Ubuntu 16.04

## Dependencies

To install Ray, first install the following dependencies. We recommend using
[Anaconda](https://www.continuum.io/downloads).

```
sudo apt-get update
sudo apt-get install -y cmake build-essential autoconf curl libtool libboost-all-dev unzip python-dev python-pip  # If you're using Anaconda, then python-dev and python-pip are unnecessary.

pip install numpy cloudpickle funcsigs colorama psutil redis
```

# Install Ray

Ray can be built from the repository as follows.

```
git clone https://github.com/ray-project/ray.git
cd ray/python
python setup.py install --user
```

## Test if the installation succeeded

To test if the installation was successful, try running some tests. This assumes
that you've cloned the git repository.

```
python test/runtest.py
```
