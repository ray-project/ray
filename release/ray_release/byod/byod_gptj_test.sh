#!/bin/bash

set -exo pipefail

pip3 install -c "$HOME/requirements_compiled.txt" myst-parser myst-nb
