#!/bin/bash
chmod +x $PWD/scripts/pre-push
ln -s $PWD/scripts/pre-push $PWD/.git/hooks/pre-push
