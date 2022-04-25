#!/usr/bin/env bash

# This script automatically download ray and run the sanity check (sanity_check.py and sanity_check_cpp.sh)
# in various Python version. This script requires conda command to exist.

unset RAY_ADDRESS
export RAY_HASH=$RAY_HASH
export RAY_VERSION=$RAY_VERSION

if [[ -z "$RAY_HASH" ]]; then
    echo "RAY_HASH env var should be provided"
    exit 1
fi

if [[ -z "$RAY_VERSION" ]]; then
    echo "RAY_VERSION env var should be provided"
    exit 1
fi

if ! [ -x "$(command -v conda)" ]; then
    echo "conda doesn't exist. Please download conda for this machine"
    exit 1
else
    echo "conda exists"
fi

echo "Start downloading Ray version ${RAY_VERSION} of commit ${RAY_HASH}"

pip install --upgrade pip

# This is required to use conda activate
source "$(conda info --base)/etc/profile.d/conda.sh"

if [[ $(uname -m) == 'arm64' ]] && [[ $OSTYPE == "darwin"* ]]; then
  PYTHON_VERSIONS=( "3.8" "3.9" )
else
  PYTHON_VERSIONS=( "3.6" "3.7" "3.8" "3.9" )
fi

for PYTHON_VERSION in "${PYTHON_VERSIONS[@]}"
do
    env_name="${RAY_VERSION}-${PYTHON_VERSION}-env"
    conda create -y -n "${env_name}" python="${PYTHON_VERSION}"
    conda activate "${env_name}"
    printf "\n\n\n"
    echo "========================================================="
    echo "Python version."
    python --version
    echo "This should be equal to ${PYTHON_VERSION}"
    echo "========================================================="
    printf "\n\n\n"

    # TODO (Alex): Get rid of this once grpc adds working PyPI wheels for M1 macs.
    if [[ $(uname -m) == 'arm64' ]] && [[ $OSTYPE == "darwin"* ]]; then
        conda install -y grpcio
    fi

    # shellcheck disable=SC2102
    pip install --index-url https://test.pypi.org/simple/ --extra-index-url https://pypi.org/simple ray[cpp]=="${RAY_VERSION}"

    failed=false
    cpp_failed=false
    printf "\n\n\n"
    echo "========================================================="
    if python sanity_check.py; then
        echo "PYTHON ${PYTHON_VERSION} succeed sanity check."
    else
        failed=true
    fi
    if bash sanity_check_cpp.sh; then
        echo "PYTHON ${PYTHON_VERSION} succeed sanity check C++."
    else
        cpp_failed=true
    fi
    echo "========================================================="
    printf "\n\n\n"
    conda deactivate
    conda remove -y --name "${env_name}" --all
    if [ "$failed" = true ]; then
        echo "PYTHON ${PYTHON_VERSION} failed sanity check."
        exit 1
    fi
    if [ "$cpp_failed" = true ]; then
        echo "PYTHON ${PYTHON_VERSION} failed sanity check C++."
        exit 1
    fi
done
