#!/bin/bash
# This script is used to build an extra layer to run release tests on Azure.

set -exo pipefail

pip3 install azure-cli-core==2.21.0 azure-core azure-identity azure-mgmt-compute azure-mgmt-network azure-mgmt-resource azure-common msrest msrestazure
