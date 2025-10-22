#!/bin/bash
# This script is used to build an extra layer to run release tests on Azure.

set -exo pipefail

pip3 install azure-cli-core<2.21.0 azure-core==1.35.0 azure-identity==1.23.1 azure-mgmt-compute==35.0.0 azure-mgmt-network==29.0.0 azure-mgmt-resource==24.0.0 azure-common==1.1.28 msrest==0.7.1 msrestazure==0.6.4.post1
