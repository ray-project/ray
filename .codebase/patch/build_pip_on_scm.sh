#!/bin/bash
source .codebase/patch/_codebase_prepare.sh
python/build-wheel-manylinux2014.sh cp36-cp36m,cp37-cp37m,cp38-cp38,cp39-cp39
cp -r .whl output/