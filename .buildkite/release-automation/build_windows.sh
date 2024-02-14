#!/bin/bash

set -ex

conda init bash
powershell ci/ray_ci/windows/cleanup.ps1
