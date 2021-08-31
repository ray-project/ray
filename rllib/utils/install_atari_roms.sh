#!/bin/bash
set -e
echo "Downloading Atari roms and importing into gym"
ROMDIR=$(mktemp -d)
pushd "$ROMDIR"
wget -q http://www.atarimania.com/roms/Roms.rar
unrar x "/Roms.rar" "$ROMDIR" > /dev/null
python -m atari_py.import_roms .
popd
rm -rf "$ROMDIR"
echo "Successfully installed Atari roms"
