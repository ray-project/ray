#!/usr/bin/env bash

if [ "${CI-}" != true ]; then
  echo "This script is meant for CI; it is dangerous to run locally. It moves all the files in the current folder."
  exit 1
fi

for rcfile in ~/.bash_profile ~/.profile ~/.bashrc; do
  if [ -f "${rcfile}" ]; then
    break
  fi
done

dir="${PWD}"
dir="${dir%/*} (project) src/${dir##*/}";

if [ "${OSTYPE}" = msys ]; then dir="${dir//\//\\}"; fi

# Put a path with spaces (and use common words like 'src') to verify correct builds
cat <<EOF >> "${rcfile}"
# Leave an empty line in the beginning here
cd "${dir}"
if [ -n "\${GITHUB_WORKSPACE-}" ]; then export GITHUB_WORKSPACE=\"\${PWD}\"; fi
if [ -n "\${TRAVIS_BUILD_DIR-}" ]; then export TRAVIS_BUILD_DIR=\"\${PWD}\"; fi
EOF

mkdir -p -- "${dir}"
for f in ./* ./.*; do
  if [ "$f" != ./. ] && [ "$f" != ./.. ] && [ -e "$f" ]; then
    mv -- "$f" "${dir}/${f#./}"  # move contents to new directory
  fi
done

cd "${dir}"
git checkout -q .  # ensure symlinks are checked out properly in new directory
echo "New workspace directory: \"${PWD}\""
