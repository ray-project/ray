#!/bin/bash

set -ex

conda init
powershell ci/ray_ci/windows/cleanup.ps1
