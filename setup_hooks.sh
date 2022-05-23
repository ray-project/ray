#!/bin/bash
chmod +x "$PWD"/ci/lint/pre-push
ln -s "$PWD"/ci/lint/pre-push "$PWD"/.git/hooks/pre-push
