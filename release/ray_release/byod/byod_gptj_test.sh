#!/bin/bash

set -exo pipefail

pip3 install -c "$HOME/requirements_compiled.txt" myst-parser myst-nb

pip3 install "accelerate==0.33.0"
pip3 uninstall -y peft
