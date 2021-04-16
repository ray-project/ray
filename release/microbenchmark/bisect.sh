#!/bin/bash

rm -f bisect_out.txt

TESTS=( "1:1 actor calls async" "multi client tasks async" )

while IFS= read -r commit; do
    WHEEL_URL="https://s3-us-west-2.amazonaws.com/ray-wheels/master/$commit/ray-2.0.0.dev0-cp37-cp37m-manylinux2014_x86_64.whl"
    pip uninstall -y ray && pip install -U "${WHEEL_URL}"

    printf "${commit}\n\n" >> bisect_out.txt
    for tst in "${TESTS[@]}"; do
      export TESTS_TO_RUN="$tst"
      OUT=`ray microbenchmark`
      printf "${OUT}\n\n" >> bisect_out.txt
    done
    printf "$\n\n" >> bisect_out.txt

done < commits.txt
