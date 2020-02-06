#!/bin/bash
chmod 0755 $PWD/scripts/pre-push
ln -s $PWD/scripts/pre-push $PWD/.git/hooks/pre-push
