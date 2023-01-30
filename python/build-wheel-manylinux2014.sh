#!/bin/bash

set -x

# Cause the script to exit if a single command fails.
set -euo pipefail

cat << EOF > "/usr/bin/nproc"
#!/bin/bash
echo 10
EOF
chmod +x /usr/bin/nproc

NODE_VERSION="14"
PYTHONS=("cp36-cp36m"
         "cp37-cp37m"
         "cp38-cp38"
         "cp39-cp39"
         "cp310-cp310"
         "cp311-cp311")

NUMPY_VERSIONS=("1.14.5"
                "1.14.5"
                "1.14.5"
                "1.19.3"
                "1.22.0"
                "1.22.0")

yum -y install unzip zip sudo
yum -y install java-1.8.0-openjdk java-1.8.0-openjdk-devel xz
yum -y install openssl

if [ "${HOSTTYPE-}" = "x86_64" ]; then
  yum install "libasan-4.8.5-44.el7.${HOSTTYPE}" -y
  yum install "libubsan-7.3.1-5.10.el7.${HOSTTYPE}" -y
  yum install "devtoolset-8-libasan-devel.${HOSTTYPE}" -y
fi

java -version
java_bin=$(readlink -f "$(command -v java)")
echo "java_bin path $java_bin"
java_home=${java_bin%jre/bin/java}
export JAVA_HOME="$java_home"

/ray/ci/env/install-bazel.sh
# Put bazel into the PATH if building Bazel from source
# export PATH=/root/bazel-3.2.0/output:$PATH:/root/bin

# If converting down to manylinux2010, the following configuration should
# be set for bazel
#echo "build --config=manylinux2010" >> /root/.bazelrc
echo "build --incompatible_linkopts_to_linklibs" >> /root/.bazelrc

if [[ -n "${RAY_INSTALL_JAVA:-}" ]]; then
  bazel build //java:ray_java_pkg
  unset RAY_INSTALL_JAVA
fi

# Install and use the latest version of Node.js in order to build the dashboard.
set +x
curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.34.0/install.sh | bash
source "$HOME"/.nvm/nvm.sh
nvm install "$NODE_VERSION"
nvm use "$NODE_VERSION"

# Build the dashboard so its static assets can be included in the wheel.
# TODO(mfitton): switch this back when deleting old dashboard code.
pushd python/ray/dashboard/client
  npm ci
  npm run build
popd
set -x

# Add the repo folder to the safe.dictory global variable to avoid the failure 
# because of secruity check from git, when executing the following command 
# `git clean ...`,  while building wheel locally.
git config --global --add safe.directory /ray

mkdir -p .whl
for ((i=0; i<${#PYTHONS[@]}; ++i)); do
  PYTHON=${PYTHONS[i]}
  NUMPY_VERSION=${NUMPY_VERSIONS[i]}

  # The -f flag is passed twice to also run git clean in the arrow subdirectory.
  # The -d flag removes directories. The -x flag ignores the .gitignore file,
  # and the -e flag ensures that we don't remove the .whl directory, the
  # dashboard directory and jars directory.
  git clean -f -f -x -d -e .whl -e python/ray/dashboard/client -e dashboard/client -e python/ray/jars

  pushd python
    # Fix the numpy version because this will be the oldest numpy version we can
    # support.
    /opt/python/"${PYTHON}"/bin/pip install -q numpy=="${NUMPY_VERSION}" cython==0.29.32
    # Set the commit SHA in __init__.py.
    if [ -n "$TRAVIS_COMMIT" ]; then
      sed -i.bak "s/{{RAY_COMMIT_SHA}}/$TRAVIS_COMMIT/g" ray/__init__.py && rm ray/__init__.py.bak
    else
      echo "TRAVIS_COMMIT variable not set - required to populated ray.__commit__."
      exit 1
    fi

    # build ray wheel
    PATH=/opt/python/${PYTHON}/bin:/root/bazel-3.2.0/output:$PATH \
    /opt/python/"${PYTHON}"/bin/python setup.py bdist_wheel
    # build ray-cpp wheel
    PATH=/opt/python/${PYTHON}/bin:/root/bazel-3.2.0/output:$PATH \
    RAY_INSTALL_CPP=1 /opt/python/"${PYTHON}"/bin/python setup.py bdist_wheel
    # In the future, run auditwheel here.
    mv dist/*.whl ../.whl/
  popd
done

# Rename the wheels so that they can be uploaded to PyPI. TODO(rkn): This is a
# hack, we should use auditwheel instead.
for path in .whl/*.whl; do
  if [ -f "${path}" ]; then
    out="${path//-linux/-manylinux2014}"
    if [ "$out" != "$path" ]; then
        mv "${path}" "${out}"
    fi
  fi
done

# Clean the build output so later operations is on a clean directory.
git clean -f -f -x -d -e .whl -e python/ray/dashboard/client
