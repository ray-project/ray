#!/usr/bin/env bash

if [ ! -z "$RAY_USE_CMAKE" ] ; then
  mvn clean install -Dmaven.test.skip
else
  bazel build //java:org_ray_ray_java_root
fi
