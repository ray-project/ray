# Installation on Mac OS X

Ray should work with Python 2. We have tested Ray on OS X 10.11.

## Dependencies

To install Ray, first install the following dependencies. We recommend using
[Anaconda](https://www.continuum.io/downloads).

```
brew update
brew install cmake automake autoconf libtool boost node
sudo easy_install pip  # If you're using Anaconda, then this is unnecessary.

pip install numpy funcsigs colorama psutil redis --ignore-installed six
pip install --upgrade git+git://github.com/cloudpipe/cloudpickle.git@0d225a4695f1f65ae1cbb2e0bbc145e10167cce4  # We use the latest version of cloudpickle because it can serialize named tuples.
pip install --upgrade --verbose "git+git://github.com/ray-project/ray.git#egg=ray&subdirectory=numbuf"
```

# Install Ray

Ray can be installed with pip as follows.

```
pip install --upgrade --verbose "git+git://github.com/ray-project/ray.git#egg=ray&subdirectory=lib/python"
```

Alternatively, Ray can be built from the repository as follows.

```
git clone https://github.com/ray-project/ray.git
cd lib/python
python setup.py install
```

## Test if the installation succeeded
To test if the installation was successful, try running some tests. This assumes
that you've cloned the git repository.

```
python test/runtest.py
```
