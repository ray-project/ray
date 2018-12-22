# Building manylinux1 wheels

To cause everything to be rebuilt, this script will delete ALL changes to the
repository, including both changes to tracked files, and ANY untracked files.

It will also cause all files inside the repository to be owned by root, and
produce .whl files owned by root.

Inside the root directory (i.e., one level above this python directory), run

```
docker run --rm -w /ray -v `pwd`:/ray -ti quay.io/xhochy/arrow_manylinux1_x86_64_base:latest /ray/python/build-wheel-manylinux1.sh
```

The wheel files will be placed in the .whl directory.

## Building MacOS wheels

To build wheels for MacOS, run the following inside the root directory (i.e.,
one level above this python directory).

```
./python/build-wheel-macos.sh
```

The script uses `sudo` multiple times, so you may need to type in a password.
