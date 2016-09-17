# Installation on Windows

Ray currently does not run on Windows. However, it can be compiled with the
following instructions.

We currently do not support Python 3.

**Note:** A batch file is provided that clones any missing third-party libraries
and applies patches to them. Do not attempt to open the solution before the
batch file applies the patches; otherwise, if the projects have been modified,
the patches may be rejected, and you may be forced to revert your changes before
re-running the batch file.

1. Install Microsoft Visual Studio 2015
2. Install Git
3. `git clone https://github.com/ray-project/ray.git`
4. `ray\thirdparty\download_thirdparty.bat`

## Test if the installation succeeded

To test if the installation was successful, try running some tests.

```
python test/runtest.py # This tests basic functionality.
python test/array_test.py # This tests some array libraries.
```
