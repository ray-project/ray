#!/bin/sh
#
##  hook prepare-commit-msg


COMMIT_MESSAGE_FILE="$1"

NAME="$(git config --get user.name)"
EMAIL="$(git config --get user.email)"

if [ -z "$NAME" ]; then
    echo "empty git config user.name"
    exit 1
fi

if [ -z "$EMAIL" ]; then
    echo "empty git config user.email"
    exit 1
fi

git interpret-trailers --if-exists doNothing --trailer \
    "Signed-off-by: $NAME <$EMAIL>" \
    --in-place "$COMMIT_MESSAGE_FILE"
