#!/bin/bash
# Sanity check that ssh tooling works inside the image.
# A too-broad LD_LIBRARY_PATH (e.g. pointing at anaconda3/lib) can shadow
# the system OpenSSL and cause ssh-keygen / ssh to segfault or fail with
# a version-mismatch error.

set -euo pipefail

echo "--- Checking ssh client version"
ssh -V

echo "--- Checking ssh-keygen (ed25519)"
ssh-keygen -t ed25519 -f /tmp/sanity_ssh_key -N "" -q
rm -f /tmp/sanity_ssh_key /tmp/sanity_ssh_key.pub

echo "--- Checking ssh-keygen (rsa)"
ssh-keygen -t rsa -b 2048 -f /tmp/sanity_ssh_key_rsa -N "" -q
rm -f /tmp/sanity_ssh_key_rsa /tmp/sanity_ssh_key_rsa.pub

echo "SSH sanity check passed."
