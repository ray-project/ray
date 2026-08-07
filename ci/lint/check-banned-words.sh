#!/usr/bin/env bash

# Checks Python and doc files for common misspellings.
#
# The correct spellings are RLlib and KubeRay. The match is case-sensitive on
# purpose: the point is to catch the wrong capitalizations, not the word.
#
# .md and .ipynb are checked alongside .rst because they are documentation
# formats too. New pages under doc/source/ must be MyST Markdown (see
# doc/test_no_new_rst.py), so .rst alone leaves the format most new prose is
# written in unchecked.

set -uo pipefail

BANNED_WORDS="RLLib Rllib Kuberay"

echo "Checking for common mis-spellings..."
found=0
for word in $BANNED_WORDS; do
    if grep -R \
        --include="*.py" \
        --include="*.rst" \
        --include="*.md" \
        --include="*.ipynb" \
        "$word" .; then
        echo "******************************"
        echo "*** Misspelled word found! ***"
        echo "******************************"
        echo "Please fix the capitalization/spelling of \"$word\" in the above files."
        found=1
    fi
done

# Report every banned word before failing. Exiting on the first one meant
# fixing a word, waiting for another full CI run, and only then learning about
# the next one.
if [[ "${found}" -ne 0 ]]; then
    exit 1
fi
