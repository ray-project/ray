#!/bin/bash

set -e

for i in {1..100}
do
   echo "Execution $i of test_many_actors.py"
   python main.py
done
