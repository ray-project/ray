#! /bin/bash

echo "Install Memory Scheduler Ray\n"
pushd /home/ubuntu/ray/python
pip install -e . --verbose
popd
