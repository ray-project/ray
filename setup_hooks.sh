#!/bin/bash
chmod +x "$PWD"/ci/pre-push
ln -s "$PWD"/ci/pre-push "$PWD"/.git/hooks/pre-push
