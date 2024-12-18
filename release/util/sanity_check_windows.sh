#!/usr/bin/env bash

# This script automatically download ray and run the sanity check for Windows
# in various Python version. This script requires conda command to exist.

export RAY_HASH="$RAY_HASH"
export RAY_VERSION="$RAY_VERSION"

if [[ -z "$RAY_HASH" ]]; then
    echo "RAY_HASH env var should be provided"
    exit 1
fi

if [[ -z "$RAY_VERSION" ]]; then
    echo "RAY_VERSION env var should be provided"
    exit 1
fi

if [[ ! -x "$(command -v conda)" ]]; then
    echo "conda doesn't exist. Please download conda for this machine"
    exit 1
else
    echo "conda exists"
fi

echo "Start downloading Ray version ${RAY_VERSION} of commit ${RAY_HASH}"

pip install --upgrade pip

# This is required to use conda activate
source "$(conda info --base)/etc/profile.d/conda.sh"

PYTHON_VERSIONS=( "3.9" "3.10" "3.11" "3.12" )

for PYTHON_VERSION in "${PYTHON_VERSIONS[@]}"; do
    ENV_NAME="${RAY_VERSION}-${PYTHON_VERSION}-env"
    conda create -y -n "${ENV_NAME}" python="${PYTHON_VERSION}"
    conda activate "${ENV_NAME}"
    printf "\n\n\n"
    echo "========================================================="
    echo "Python version."
    python --version
    echo "This should be equal to ${PYTHON_VERSION}"
    echo "========================================================="
    printf "\n\n\n"

    pip install --index-url https://test.pypi.org/simple/ --extra-index-url https://pypi.org/simple "ray[cpp]==${RAY_VERSION}"

    FAILED="false"
    printf "\n\n\n"
    echo "========================================================="
    if python sanity_check.py --ray_version="$RAY_VERSION" --ray_commit="$RAY_HASH"; then
        echo "PYTHON ${PYTHON_VERSION} succeed sanity check."
    else
        FAILED="true"
    fi
    echo "========================================================="
    printf "\n\n\n"
    conda deactivate
    conda remove -y --name "${ENV_NAME}" --all
    if [[ "$FAILED" == "true" ]]; then
        echo "PYTHON ${PYTHON_VERSION} failed sanity check."
        exit 1
    fi
done
