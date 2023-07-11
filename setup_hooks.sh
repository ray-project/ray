#!/bin/sh
chmod +x "$PWD"/ci/v2/lint/pre-push
ln -s "$PWD"/ci/v2/lint/pre-push "$PWD"/.git/hooks/pre-push
ln -s "$PWD"/ci/v2/lint/prepare-commit-msg "$PWD"/.git/hooks/prepare-commit-msg
