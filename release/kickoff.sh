#!/usr/bin/env bash


source "$2"

ray_version=${ray_version:-}
commit=${commit:-}

if [[ $ray_version == "" || $commit == "" || $1 == "" ]]
then
    echo "Provide --ray-version, --commit, and --ray-branch"
    exit 1
fi

echo "version: $ray_version"
echo "commit: $commit"
echo "workload: $1"

DATESTR=$(date +%Y%m%d-%H%M)
SESSION="$1-$DATESTR"

echo "session: $SESSION"

chmod +x ./run.sh
if [ -z "$NO_UP" ]; then
  anyscale up "$SESSION"
fi
anyscale push "$SESSION"
anyscale exec -n "$SESSION" "./run.sh $1 --ray-version=$ray_version --commit=$commit"
