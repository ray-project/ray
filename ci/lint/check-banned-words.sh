#!/usr/bin/env bash

# Checks Python and doc files for common mispellings.

BANNED_WORDS="RLLib Rllib"

echo "Checking for common mis-spellings..."
for word in $BANNED_WORDS; do
    if grep -R --include="*.py" --include="*.rst" "$word" .; then
        echo "******************************"
        echo "*** Misspelled word found! ***"
        echo "******************************"
        echo "Please fix the capitalization/spelling of \"$word\" in the above files."
        exit 1
    fi
done