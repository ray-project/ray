#!/bin/bash

cat << EOF > "/usr/bin/nproc"
#!/bin/bash
echo 10
EOF
chmod +x /usr/bin/nproc

PYTHONS=("cp27-cp27mu"
         "cp35-cp35m"
         "cp36-cp36m"
         "cp37-cp37m")

# On python 3.7, a newer version of numpy seems to be necessary.
NUMPY_VERSIONS=("1.10.4"
                "1.10.4"
                "1.10.4"
                "1.14.5")

sudo apt-get install unzip
/ray/ci/travis/install-bazel.sh

# Put bazel into the PATH
export PATH=$PATH:/root/bin

# Remove this old Python 2.4.3 executable, and make the "python2" command find
# a newer version of Python. We need this for autogenerating some files for the
# UI.
rm -f /usr/bin/python2
ln -s /opt/python/cp27-cp27m/bin/python2 /usr/bin/python2

mkdir .whl
for ((i=0; i<${#PYTHONS[@]}; ++i)); do
  PYTHON=${PYTHONS[i]}
  NUMPY_VERSION=${NUMPY_VERSIONS[i]}
  # The -f flag is passed twice to also run git clean in the arrow subdirectory.
  # The -d flag removes directories. The -x flag ignores the .gitignore file,
  # and the -e flag ensures that we don't remove the .whl directory.
  git clean -f -f -x -d -e .whl
  pushd python
    # Fix the numpy version because this will be the oldest numpy version we can
    # support.
    /opt/python/${PYTHON}/bin/pip install -q numpy==${NUMPY_VERSION} cython==0.29.0
    PATH=/opt/python/${PYTHON}/bin:$PATH /opt/python/${PYTHON}/bin/python setup.py bdist_wheel
    # In the future, run auditwheel here.
    mv dist/*.whl ../.whl/
  popd
done

# Rename the wheels so that they can be uploaded to PyPI. TODO(rkn): This is a
# hack, we should use auditwheel instead.
pushd .whl
  find *.whl -exec bash -c 'mv $1 ${1//linux/manylinux1}' bash {} \;
popd
