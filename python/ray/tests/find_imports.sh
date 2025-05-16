#!/bin/bash

# Description:
# This script finds all Python files in the current directory (recursively)
# and prints the names of those where the line immediately following
# `if __name__ == '__main__':` is an import statement.

find . -name "*.py" | while read -r file; do
    # Use awk to read through the file line by line
    awk '
        found == 1 && $0 ~ /^[[:space:]]*import[[:space:]]+/ {
            print FILENAME
            exit
        }
        found == 1 && $0 ~ /^[[:space:]]*from[[:space:]]+[a-zA-Z0-9_.]+[[:space:]]+import/ {
            print FILENAME
            exit
        }
        $0 ~ /^if __name__ == .__main__.:/ {
            found = 1
            next
        }
        found == 1 { exit }
    ' "$file"
done

