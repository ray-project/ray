#!/usr/bin/env bash

# Installs serve dependencies ("ray[serve]") on top of minimal install

# Get script's directory: https://stackoverflow.com/a/246128
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

# Installs minimal dependencies
"$SCRIPT_DIR"/install-minimal.sh

# Installs serve dependencies
python -m pip install -U "ray[serve]"
