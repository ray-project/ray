# Building manylinux2014 wheels

**WARNING:** To cause everything to be rebuilt, this script will delete ALL changes to the
repository, including both changes to tracked files, and ANY untracked files.

It will also cause all files inside the repository to be owned by root, and
produce .whl files owned by root.

Inside the root directory (i.e., one level above this python directory), run

```
docker run -ti --rm \
    -e HOST_UID=$(id -u) \
    -e HOST_GID=$(id -g) \
    -e BUILDKITE_COMMIT="$(git rev-parse HEAD)" \
    -e BUILD_ONE_PYTHON_ONLY=py39 \
    -w /ray -v "$(pwd)":/ray \
    -e HOME=/tmp \
    quay.io/pypa/manylinux2014_x86_64:2024-07-02-9ac04ee \
    /ray/python/build-wheel-manylinux2014.sh
```

The Python 3.9 wheel files will be placed in the `.whl` directory.

One can change the value of `BUILDKITE_COMMIT` to generate wheels with
different built-in commit string (the code is not changed) and
`BUILD_ONE_PYTHON_ONLY` to build wheels of different Python versions.

For arm64 / aarch64 architecture, use the `quay.io/pypa/manylinux2014_aarch64`
image:

```
docker run -ti --rm \
    -e BUILDKITE_COMMIT="$(git rev-parse HEAD)" \
    -e BUILD_ONE_PYTHON_ONLY=py39 \
    -w /ray -v "$(pwd)":/ray \
    quay.io/pypa/manylinux2014_aarch64:2024-07-02-9ac04ee \
    /ray/python/build-wheel-manylinux2014.sh
```

## Building MacOS wheels

To build wheels for MacOS, run the following inside the root directory (i.e.,
one level above this python directory).

```
./python/build-wheel-macos.sh
```

The script uses `sudo` multiple times, so you may need to type in a password.
