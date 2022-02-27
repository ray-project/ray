#!/bin/bash
set -e
echo "Downloading Atari roms and importing into gym"
ROMDIR=$(mktemp -d)
pushd "$ROMDIR"
wget -q http://www.atarimania.com/roms/Roms.rar
unrar x "Roms.rar" > /dev/null
# Importing from zipfiles with atari_py only works in Python >= 3.7
# so we unzip manually here
mkdir -p ./unzipped
unzip -qq "ROMS.zip" -d ./unzipped
unzip -qq "HC ROMS.zip" -d ./unzipped
python -m atari_py.import_roms ./unzipped
popd
echo rm -rf "$ROMDIR"
echo "Successfully installed Atari roms"
