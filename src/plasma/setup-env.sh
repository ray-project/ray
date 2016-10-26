echo "Adding Plasma to PYTHONPATH" 1>&2

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)

export PYTHONPATH="$ROOT_DIR/lib/python/:$PYTHONPATH"
