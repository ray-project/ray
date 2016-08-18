# NO shebang! Force the user to run this using the 'source' command without spawning a new shell; otherwise, variable exports won't persist.

echo "Adding Ray to PYTHONPATH" 1>&2

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)

export PYTHONPATH="$ROOT_DIR/lib/python/:$ROOT_DIR/thirdparty/numbuf/build:$PYTHONPATH"

# Print instructions for adding Ray to your bashrc.
unamestr="$(uname)"
if [[ "$unamestr" == "Linux" ]]; then
  BASH_RC="~/.bashrc"
elif [[ "$unamestr" == "Darwin" ]]; then
  BASH_RC="~/.bash_profile"
fi
echo "To permanently add Ray to your Python path, run,

echo 'export PYTHONPATH=$ROOT_DIR/lib/python/:$ROOT_DIR/thirdparty/numbuf/build:\$PYTHONPATH' >> $BASH_RC
"
