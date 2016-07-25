# NO shebang! Force the user to run this using the 'source' command without spawning a new shell; otherwise, variable exports won't persist.

echo "Adding Ray to PYTHONPATH" 1>&2

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)

export PYTHONPATH="$ROOT_DIR/lib/python/:$ROOT_DIR/thirdparty/numbuf/build"
