#!/usr/bin/env bash

git_sha=""

usage() {
    echo "Run ASAN tests."
}

for i in "$@"
do
case "$i" in
    --git-sha=*)
    git_sha="${i#*=}"
    ;;
    --help)
    usage
    exit
    ;;
    *)
    echo "unknown arg, $2"
    exit 1
    ;;
esac
done

echo "git-sha: $git_sha"

./run_asan_tests.sh setup
if [ -n "${git_sha}" ]
then
    git_sha="${git_sha}" ./run_asan_tests.sh recompile
fi
./run_asan_tests.sh run
