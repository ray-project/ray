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
PYTHONS=("3.7"
         "3.8"
         "3.9")
NUMPY_VERSIONS=("1.14.5"
                "1.14.5"
                "1.19.3")

export DEBIAN_FRONTEND=noninteractive
export TZ=UTC
apt update && apt install -y lsb-release wget software-properties-common unzip zip curl git

/ray/ci/env/install-llvm-binaries.sh
/ray/ci/env/install-bazel.sh

echo "build --incompatible_linkopts_to_linklibs" >> /root/.bazelrc

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

wget --quiet "https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh" -O /tmp/miniconda.sh \
    && /bin/bash /tmp/miniconda.sh -b -u -p $HOME/anaconda3 \
    && $HOME/anaconda3/bin/conda init \ 
    && rm /tmp/miniconda.sh

mkdir -p .whl
for ((i=0; i<${#PYTHONS[@]}; ++i)); do
  PYTHON=${PYTHONS[i]}
  NUMPY_VERSION=${NUMPY_VERSIONS[i]}

  # The -f flag is passed twice to also run git clean in the arrow subdirectory.
  # The -d flag removes directories. The -x flag ignores the .gitignore file,
  # and the -e flag ensures that we don't remove files needed for the build process
  # or included in the wheels.
  git config --global --add safe.directory /ray
  git clean -f -f -x -d -e .whl -e python/ray/dashboard/client -e dashboard/client -e python/ray/jars -e .llvm-local.bazelrc

  $HOME/anaconda3/bin/conda deactivate && $HOME/anaconda3/bin/conda create -n $RANDOM \
      $HOME/anaconda3/bin/conda install python=${PYTHON}
  
  python --version

  pushd python
    # Fix the numpy version because this will be the oldest numpy version we can
    # support.
    pip install -q numpy=="${NUMPY_VERSION}" cython==0.29.26
    # Set the commit SHA in __init__.py.
    if [ -n "$TRAVIS_COMMIT" ]; then
      sed -i.bak "s/{{RAY_COMMIT_SHA}}/$TRAVIS_COMMIT/g" ray/__init__.py && rm ray/__init__.py.bak
    else
      echo "TRAVIS_COMMIT variable not set - required to populated ray.__commit__."
      exit 1
    fi

    # build ray wheel
    python setup.py bdist_wheel
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
