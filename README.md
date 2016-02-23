# Orchestra

Orchestra is a distributed execution framework with a Python-like programming model.

## Setup

**Install GRPC**

1. Follow the instructions [here](https://github.com/grpc/grpc/blob/master/INSTALL), though some of the instructions are outdated.
2. `cd ~/grpc`
3. `mkdir build`
4. `cd build`
5. `cmake ..`
6. `make`
7. `make install`
8. `cd ..`
9. `python setup.py install`

**Install Orchestra**

1. `git clone git@github.com:amplab/orch.git`
2. `cd orch`
3. `mkdir build`
4. `cd build`
5. `cmake ..`
6. `make install`
7. `cd ../lib/orchpy`
8. `python setup.py install`
9. `cd ~/orch/test`
10. `bash gen-python-code.sh`
11. `python runtest.py`
