#!/usr/bin/env bash
#first you should run the ../test.sh to build
ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)
export RAY_CONFIG=$ROOT_DIR/../ray.config.ini
java -Djava.library.path=../../build/src/plasma:../../build/src/local_scheduler -cp .:target/ray-example-1.0.jar:lib/* org.ray.example.HelloWorld