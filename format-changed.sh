#!/usr/bin/env bash

# Move to top-level directory.
cd $(git rev-parse --show-toplevel)

BLACK_VERSION_REQUIRED="21.12b0"

# Check if Black exists
if ! [ -x "$(command -v black)" ]; then
        echo "Black is not installed."
        echo "Run pip install black==$BLACK_VERSION_REQUIRED"
        exit 1
fi

BLACK_VERSION=$(black --version | awk '{print $2}')

# Check if Black version is correct
if [ "$BLACK_VERSION" != "$BLACK_VERSION_REQUIRED" ]; then
    echo "Expected black==$BLACK_VERSION_REQUIRED but got black==$BLACK_VERSION."
    echo "Run pip install -I black==$BLACK_VERSION_REQUIRED"
    exit 1
fi

BLACK_EXCLUDES=(
    '--extend-exclude' 'python/ray/cloudpickle/*'
    '--extend-exclude' 'python/build/*'
    '--extend-exclude' 'python/ray/core/src/ray/gcs/*'
    '--extend-exclude' 'python/ray/thirdparty_files/*'
    '--extend-exclude' 'python/ray/_private/thirdparty/*'
)

# Format changed files.
MERGEBASE="$(git merge-base upstream/master HEAD)"

if ! git diff --diff-filter=ACRM --quiet --exit-code "$MERGEBASE" -- '*.py' &>/dev/null; then
    git diff --name-only --diff-filter=ACRM "$MERGEBASE" -- '*.py' | xargs -P 5 \
        black "${BLACK_EXCLUDES[@]}"
fi