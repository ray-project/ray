#!/bin/bash
# This script is used to build an extra layer to run release tests on Azure.

set -exo pipefail

PIP_PKGS=(
    "azure-cli-core==2.21.0"
    azure-core
    azure-identity
    azure-mgmt-compute
    azure-mgmt-network
    azure-mgmt-resource
    azure-common
    msrest
    msrestazure
)

pip3 install "${PIP_PKGS[@]}"
