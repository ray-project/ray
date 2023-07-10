#!/bin/sh
chmod +x "$PWD"/ci/lint/pre-push
ln -s "$PWD"/ci/lint/pre-push "$PWD"/.git/hooks/pre-push
ln -s "$PWD"/ci/lint/prepare-commit-msg "$PWD"/.git/hooks/prepare-commit-msg
