#!/bin/bash

# Helper script to use kubectl as a remote shell for rsync to sync files
# to/from pods that have rsync installed. Taken from:
# https://serverfault.com/questions/741670/rsync-files-to-a-kubernetes-pod/746352

if [ -z "$KRSYNC_STARTED" ]; then
    export KRSYNC_STARTED=true
    exec rsync --blocking-io --rsh "$0" "$@"
fi

# Running as --rsh
namespace=''
pod=$1
shift

# If use uses pod@namespace rsync passes as: {us} -l pod namespace ...
if [ "X$pod" = "X-l" ]; then
    pod=$1
    shift
    # Space before $1 leads to namespace errors
    namespace="-n$1"
    shift
fi

if [ -z "$KUBE_API_SERVER" ]; then
  exec kubectl "$namespace" exec -i "$pod" -- "$@"
else
  exec kubectl --server "$KUBE_API_SERVER" "$namespace" exec -i "$pod" -- "$@"
fi
