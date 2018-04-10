#!/bin/bash
set -x

# Git pre-commit hook to check staged Python files for formatting issues with
# yapf.
#
# INSTALLING: Copy this script into `.git/hooks/pre-commit`, and mark it as
# executable.
#
# This requires that yapf is installed and runnable in the environment running
# the pre-commit hook.
#
# When running, this first checks for unstaged changes to staged files, and if
# there are any, it will exit with an error. Files with unstaged changes will be
# printed.
#
# If all staged files have no unstaged changes, it will run yapf against them,
# leaving the formatting changes unstaged. Changed files will be printed.
#
# BUGS: This does not leave staged changes alone when used with the -a flag to
# git commit, due to the fact that git stages ALL unstaged files when that flag
# is used.

# Copied from .travis/check-git-clang-format-output.sh.
if [ "$TRAVIS_PULL_REQUEST" == "false" ] ; then
  # Not in a pull request, so compare against parent commit
  base_commit="HEAD^"
  echo "Running yapf against parent commit $(git rev-parse $base_commit)"
else
  base_commit="$TRAVIS_BRANCH"
  echo "Running yapf against branch $base_commit, with hash $(git rev-parse $base_commit)"
fi

# Find all staged Python files, and exit early if there aren't any.
PYTHON_FILES=(`git diff ${base_commit} --name-only --cached --diff-filter=AM | grep --color=never '.py$'`)
if [ ! "$PYTHON_FILES" ]; then
  exit 0
fi

# Verify that yapf is installed; if not, warn and exit.
if [ -z $(which yapf) ]; then
  echo 'yapf not on path; can not format. Please install yapf:'
  echo '    pip install yapf'
  exit 2
fi

# Check for unstaged changes to files in the index.
CHANGED_FILES=(`git diff --name-only ${PYTHON_FILES[@]}`)
if [ "$CHANGED_FILES" ]; then
  echo 'You have unstaged changes to some files in your commit; skipping '
  echo 'auto-format. Please stage, stash, or revert these changes. You may '
  echo 'find `git stash -k` helpful here.'
  echo
  echo 'Files with unstaged changes:'
  for file in ${CHANGED_FILES[@]}; do
    echo "  $file"
  done
  exit 1
fi
# Format all staged files, then exit with an error code if any have uncommitted
# changes.
echo 'Formatting staged Python files . . .'
yapf -i -r ${PYTHON_FILES[@]}
CHANGED_FILES=(`git diff --name-only ${PYTHON_FILES[@]}`)
if [ "$CHANGED_FILES" ]; then
  echo 'Reformatted staged files. Please review and stage the changes.'
  echo
  echo 'Files updated:'
  for file in ${CHANGED_FILES[@]}; do
    echo "  $file"
  done
  exit 1
else
  exit 0
fi
