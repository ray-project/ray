# Orchestra

Orchestra is a distributed execution framework with a Python-like programming model.

## Design Decisions

For a description of our design decisions, see

- [Reference Counting](doc/reference-counting.md)
- [Aliasing](doc/aliasing.md)

## Setup

**Install Arrow**

1. `git clone https://github.com/apache/arrow.git`
2. `cd ~/arrow`
3. `git checkout 2d8627cd81f83783b0ceb01d137a46b581ecba26`
4. `cd ~/arrow/cpp`
5. `bash setup_build_env.sh`
6. `cd ~/arrow/cpp/thirdparty/flatbuffers-1.3.0`
7. `export FLATBUFFERS_HOME=~/arrow/cpp/thirdparty/installed` (or equivalent)
8. `cd ~/arrow/cpp/build`
9. `cmake ..`
10. `make`
11. `sudo make install`
12. add `export LD_LIBRARY_PATH=LD_LIBRARY_PATH:/usr/local/lib` to your `~/.bashrc`
13. `source ~/.bashrc`

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
