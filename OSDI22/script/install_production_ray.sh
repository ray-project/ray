#! /bin/bash

echo "Install Production Ray\n"
pushd /home/ubuntu/ray_production/python
pip install -e . --verbose
popd
