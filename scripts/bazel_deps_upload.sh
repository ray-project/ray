#!/bin/bash

#OSS_ID: the access key ID, come from the environment variable.
#OSS_KEY: the access key secret, come from the environment variable.
#OSS_HOST: the endpoint of the OSS, come from the environment variable.

https_url=$1
if [[ $https_url == http://* ]]; then
    oss_url=${https_url//http:\/\//oss:\/\/raylet\/ci\/bazel\/deps\/}
elif [[ $https_url == https://* ]]; then
    oss_url=${https_url//https:\/\//oss:\/\/raylet\/ci\/bazel\/deps\/}
else
    echo "[FAILED] The url ${https_url} must start with 'http://' or 'https://'"
    exit 1
fi

echo "$https_url -> $oss_url"
if osscmd info "$oss_url" --id="$OSS_ID" --key="$OSS_KEY" --host="$OSS_HOST" > /dev/null 2>&1; then
    echo "[OK] The target ${oss_url} is already exists."
    exit 0
fi

filename=$(basename "$https_url")
if ! wget "$https_url" -O "$filename"; then
    echo "[FAILED] Failed to download ${https_url}"
    exit 1
fi

osscmd put "$filename" "$oss_url" --id="$OSS_ID" --key="$OSS_KEY" --host="$OSS_HOST"
