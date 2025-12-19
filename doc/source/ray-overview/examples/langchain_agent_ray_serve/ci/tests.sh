#!/bin/bash

# Don't use nbconvert or jupytext unless you're willing
# to check each subprocess unit and validate that errors
# aren't being consumed/hidden.

set -euxo pipefail

# Convert and run the README notebook (skip deployment commands)
python ci/nb2py.py "content/README.ipynb" "content/README.py" --ignore-cmds
#python "content/README.py"
#rm "content/README.py"
