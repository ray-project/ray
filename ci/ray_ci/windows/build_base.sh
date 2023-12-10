#!/bin/bash

set -ex

conda init 
pip install -U --ignore-installed  \
  -c python/requirements_compiled.txt \
  -r python/requirements.txt \
  -r python/requirements/test-requirements.txt

# Clean up temp files to speed up docker build
pip cache purge
powershell ci/ray_ci/windows/cleanup.ps1
