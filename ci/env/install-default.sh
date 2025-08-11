#!/usr/bin/env bash

# Installs default dependencies ("ray[default]") on top of minimal install

# Get script's directory: https://stackoverflow.com/a/246128
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

# Installs minimal dependencies
"$SCRIPT_DIR"/install-minimal.sh

# Installs default dependencies
python -m pip install -U "ray[default]"
