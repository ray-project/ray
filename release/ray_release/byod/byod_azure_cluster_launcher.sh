#!/bin/bash
# This script is used to build an extra layer to run release tests on Azure.

set -exo pipefail

pip3 install azure-identity azure-mgmt-network azure-mgmt-compute azure-common msrestazure
