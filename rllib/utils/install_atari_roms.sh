#!/bin/bash

ROMDIR=$(mktemp -d)
wget -q -O "$ROMDIR/Roms.rar" http://www.atarimania.com/roms/Roms.rar
unrar x "$ROMDIR/Roms.rar" "$ROMDIR" > /dev/null
python -m atari_py.import_roms "$ROMDIR"
rm -rf "$ROMDIR"
