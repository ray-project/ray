#!/bin/bash

set -ex

powershell ci/pipeline/fix-windows-container-networking.ps1
pip install awscli
