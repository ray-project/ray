#!/bin/bash

set -x

# Cause the script to exit if a single command fails.
set -e

cat << EOF > "/usr/bin/nproc"
#!/bin/bash
echo 10
EOF
chmod +x /usr/bin/nproc

PYTHONS=("cp35-cp35m"
         "cp36-cp36m"
         "cp37-cp37m"
         "cp38-cp38")

# The minimum supported numpy version is 1.14, see
# https://issues.apache.org/jira/browse/ARROW-3141
NUMPY_VERSIONS=("1.14.5"
                "1.14.5"
                "1.14.5"
                "1.14.5")

sudo apt-get install unzip
/ray/ci/travis/install-bazel.sh

# Put bazel into the PATH
export PATH=$PATH:/root/bin

# Install and use the latest version of Node.js in order to build the dashboard.
set +x
curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.34.0/install.sh | bash
source $HOME/.nvm/nvm.sh
nvm install node
nvm use node

# Build the dashboard so its static assets can be included in the wheel.
pushd python/ray/dashboard/client
  npm ci
  npm run build
popd
set -x

mkdir .whl
for ((i=0; i<${#PYTHONS[@]}; ++i)); do
  PYTHON=${PYTHONS[i]}
  NUMPY_VERSION=${NUMPY_VERSIONS[i]}

  # The -f flag is passed twice to also run git clean in the arrow subdirectory.
  # The -d flag removes directories. The -x flag ignores the .gitignore file,
  # and the -e flag ensures that we don't remove the .whl directory and the
  # dashboard directory.
  git clean -f -f -x -d -e .whl -e python/ray/dashboard/client

  pushd python
    # Fix the numpy version because this will be the oldest numpy version we can
    # support.
    /opt/python/${PYTHON}/bin/pip install -q numpy==${NUMPY_VERSION} cython==0.29.15
    # Set the commit SHA in __init__.py.
    if [ -n "$TRAVIS_COMMIT" ]; then
      sed -i.bak "s/{{RAY_COMMIT_SHA}}/$TRAVIS_COMMIT/g" ray/__init__.py && rm ray/__init__.py.bak
    else
      echo "TRAVIS_COMMIT variable not set - required to populated ray.__commit__."
      exit 1
    fi

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
