# CI Docker files

This directory contains the Dockerfiles used to build the CI system.

These are _not_ the Dockerfiles that build the `rayproject/ray` releases. These 
are found in the `/docker` directory under the root.

The Dockerfiles are hierarchical and will be built in different places during a CI run.

## Base images

The base images are built per-branch either when they are first requested or on a periodic basis
(for the master branch). The base images contain the latest dependencies of the respective branch.
Every per-commit build will always install the latest dependencies to make sure everything is up to date.
However, by using the base images as a source, this will mostly be a no or low cost operation.

- `base.test.Dockerfile` contains common dependencies for all images
- `base.build.Dockerfile` inherits from `base.test` and installs build dependencies like Java and LLVM
- `base.ml.Dockerfile` inherits from `base.test` and installs ML dependencies like torch/tensorflow
- `base.gpu.Dockerfile` inherits from a CUDA base image and otherwise contains the same content as `base.test` and `base.ml`.

## Per-commit images

On every commit, the following images are built in this order:

- `build.Dockerfile` (based on `base.build`) which will build the Ray binaries
- `test.Dockerfile` (based on `base.test`), where we will inject the built Ray libraries
- `ml.Dockerfile` (based on `base.ml`), where we will inject the built Ray libraries
- `gpu.Dockerfile` (based on `base.ml`), where we will inject the built Ray libraries
