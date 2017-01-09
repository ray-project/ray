# Installation on Mac OS X

Ray should work with Python 2 and Python 3. We have tested Ray on OS X 10.11.

## Dependencies

To install Ray, first install the following dependencies. We recommend using
[Anaconda](https://www.continuum.io/downloads).

```
brew update
brew install cmake automake autoconf libtool boost wget
sudo easy_install pip  # If you're using Anaconda, then this is unnecessary.

pip install numpy cloudpickle funcsigs colorama psutil redis --ignore-installed six
pip install --upgrade --verbose "git+git://github.com/ray-project/ray.git#egg=numbuf&subdirectory=numbuf"
```

# Install Ray

Ray can be built from the repository as follows.

```
git clone https://github.com/ray-project/ray.git
cd ray/lib/python
python setup.py install --user
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
