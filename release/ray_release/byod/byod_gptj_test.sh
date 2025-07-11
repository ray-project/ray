#!/bin/bash

set -exo pipefail

pip3 install -c "$HOME/requirements_compiled.txt" myst-parser myst-nb

# TODO(matthewdeng): upgrade datasets globally
pip3 install datasets==3.6.0
