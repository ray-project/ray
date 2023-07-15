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

# Python version key, interpreter version code, numpy tuples.
PYTHON_NUMPYS=(
  "py37 cp37-cp37m 1.14.5"
  "py38 cp38-cp38 1.14.5"
  "py39 cp39-cp39 1.19.3"
  "py310 cp310-cp310 1.22.0"
  "py311 cp311-cp311 1.22.0"
)

YUM_PKGS=(
  unzip zip sudo openssl xz
  java-1.8.0-openjdk java-1.8.0-openjdk-devel maven
)

if [[ "${HOSTTYPE-}" == "x86_64" ]]; then
  YMB_PKGS+=(
    "libasan-4.8.5-44.el7.x86_64"
    "libubsan-7.3.1-5.10.el7.x86_64"
    "devtoolset-8-libasan-devel.x86_64"
  )
fi

yum -y install "${YUM_PKGS[@]}"

java -version
JAVA_BIN="$(readlink -f "$(command -v java)")"
echo "java_bin path ${JAVA_BIN}"
export JAVA_HOME="${JAVA_BIN%jre/bin/java}"

/ray/ci/env/install-bazel.sh
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
(
  cd python/ray/dashboard/client
  npm ci
  npm run build
)
set -x

# Add the repo folder to the safe.dictory global variable to avoid the failure
# because of secruity check from git, when executing the following command
# `git clean ...`,  while building wheel locally.
git config --global --add safe.directory /ray

mkdir -p .whl
for PYTHON_NUMPY in "${PYTHON_NUMPYS[@]}" ; do
  PYTHON_VERSION_KEY="$(echo "${PYTHON_NUMPY}" | cut -d' ' -f1)"
  if [[ "${BUILD_ONE_PYTHON_ONLY:-}" != "" && "${PYTHON_VERSION_KEY}" != "${BUILD_ONE_PYTHON_ONLY}" ]]; then
    continue
  fi

  PYTHON="$(echo "${PYTHON_NUMPY}" | cut -d' ' -f2)"
  NUMPY_VERSION="$(echo "${PYTHON_NUMPY}" | cut -d' ' -f3)"

  echo "--- Build wheel for ${PYTHON}, numpy=${NUMPY_VERSION}"

  # The -f flag is passed twice to also run git clean in the arrow subdirectory.
  # The -d flag removes directories. The -x flag ignores the .gitignore file,
  # and the -e flag ensures that we don't remove the .whl directory, the
  # dashboard directory and jars directory, as well as the compiled
  # dependency constraints.
  git clean -f -f -x -d -e .whl -e python/ray/dashboard/client -e dashboard/client -e python/ray/jars -e python/requirements_compiled.txt

  (
    cd python
    # Fix the numpy version because this will be the oldest numpy version we can
    # support.
    /opt/python/"${PYTHON}"/bin/pip install -q numpy=="${NUMPY_VERSION}" cython==0.29.32
    # Set the commit SHA in __init__.py.
    if [[ -n "$TRAVIS_COMMIT" ]]; then
      sed -i.bak "s/{{RAY_COMMIT_SHA}}/$TRAVIS_COMMIT/g" ray/__init__.py && rm ray/__init__.py.bak
    else
      echo "TRAVIS_COMMIT variable not set - required to populated ray.__commit__."
      exit 1
    fi

    # build ray wheel
    PATH="/opt/python/${PYTHON}/bin:$PATH" \
    "/opt/python/${PYTHON}/bin/python" setup.py -q bdist_wheel

    # build ray-cpp wheel
    PATH="/opt/python/${PYTHON}/bin:$PATH" \
    RAY_INSTALL_CPP=1 "/opt/python/${PYTHON}/bin/python" setup.py -q bdist_wheel

    # In the future, run auditwheel here.
    mv dist/*.whl ../.whl/
  )
done

# Rename the wheels so that they can be uploaded to PyPI. TODO(rkn): This is a
# hack, we should use auditwheel instead.
for path in .whl/*.whl; do
  if [[ -f "${path}" ]]; then
    out="${path//-linux/-manylinux2014}"
    if [[ "$out" != "$path" ]]; then
        mv "${path}" "${out}"
    fi
  fi
done

# Clean the build output so later operations is on a clean directory.
git clean -f -f -x -d -e .whl -e python/ray/dashboard/client -e python/requirements_compiled.txt

echo "--- Build JAR"
if [ "${BUILD_JAR-}" == "1" ]; then
  ./java/build-jar-multiplatform.sh linux
fi
