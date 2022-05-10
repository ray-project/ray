#!/usr/bin/env bash

set -e

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)

function usage()
{
  echo "Usage: copyright-format.sh [<args>]"
  echo
  echo "Options:"
  echo "  -h|--help               print the help info"
  echo "  -c|--check              check whether there are format issues in C++ files"
  echo "  -f|--fix                fix all the format issue directly"
  echo
}

pushd "$ROOT_DIR"/../..

COPYRIGHT_FILE="$ROOT_DIR"/default-copyright.txt

COPYRIGHT=$(cat "$COPYRIGHT_FILE")

LINES_NUM=$(echo "$COPYRIGHT" | wc -l)

DEFAULT_YEAR="2017"

# The key words that must included in copyright description.
KEYWORDS=(
    "Copyright"
    "Ray Authors"
    "License"
)

RUN_TYPE="diff"

FILE_LIST_TMP_FILE="/tmp/.cr_file_list_tmp"

TMP_FILE="/tmp/.cr_tmp"

# The cpp files that will be checked and formatted.
INCLUDES_CPP_DIRS=(
    src
    cpp
)

# The files that should be skipped.
EXCLUDES_DIRS=(
    src/ray/object_manager/plasma/
    src/ray/thirdparty
    cpp/example
)

ERROR_FILES=()

# Parse options
while [ $# -gt 0 ]; do
  key="$1"
  case $key in
    -h|--help)
      usage
      exit 0
      ;;
    -c|--check)
      RUN_TYPE="diff"
      ;;
    -f|--fix)
      RUN_TYPE="fix"
      ;;
    *)
      echo "ERROR: unknown option \"$key\""
      echo
      usage
      exit 1
      ;;
  esac
  shift
done

for directory in "${INCLUDES_CPP_DIRS[@]}"; do
    cmd_args="find $directory -type f"
    for excluded in "${EXCLUDES_DIRS[@]}"; do
        cmd_args="${cmd_args} ! -path " 
        cmd_args="${cmd_args} '${excluded}"
        if [[ "${excluded: -1}" != "/" ]];then
            cmd_args="${cmd_args}/"
        fi
        cmd_args="${cmd_args}*'"
    done
    cmd_args="${cmd_args} \( -name '*.cc' -or -name '*.h' -or -name '*.proto' \)"
    eval "${cmd_args}" > "$FILE_LIST_TMP_FILE"
    while IFS=$'\n' read -r f
    do
        head_content=$(sed -n "1,${LINES_NUM}p" "$f")
        match="true"
        for key_word in "${KEYWORDS[@]}"; do
            if [[ $head_content != *$key_word* ]];then
                match="false"
                break
            fi
        done
        if [[ "$match" == "false" ]]; then
            ERROR_FILES+=("$f")
            if [[ "$RUN_TYPE" == "fix" ]];then
                first_added_year=$(git log --pretty=format:%ad --date=short "$f" | awk -F- '{print $1}' | tail -1)
                last_modified_year=$(git log --pretty=format:%ad --date=short "$f" | awk -F- '{print $1}' | head -1)
                years=""
                if [[ "$first_added_year" == "$last_modified_year" ]];then
                    years=$first_added_year
                else
                    years="${first_added_year}-${last_modified_year}"
                fi
                sed '1s/^/\n/' "$f" > $TMP_FILE
                mv $TMP_FILE "$f"
                cat "$COPYRIGHT_FILE" "$f" > $TMP_FILE
                mv $TMP_FILE "$f"
                sed "s/${DEFAULT_YEAR}/${years}/" "$f" > $TMP_FILE
                mv $TMP_FILE "$f"
            fi
        fi
    done < $"$FILE_LIST_TMP_FILE"
    rm -f "$FILE_LIST_TMP_FILE"
done

if [[ ${#ERROR_FILES[*]} -gt 0 ]];then
    if [[ "$RUN_TYPE" == "fix" ]];then
        echo "Copyright has been added to the files below:"
        printf '%s\n' "${ERROR_FILES[@]}"
        exit 0
    else
        echo "Missing copyright info at the beginning of below files. Please run 'sh ci/lint/copyright-format.sh -f' to fix them:"
        printf '%s\n' "${ERROR_FILES[@]}"
        exit 1
    fi
else
    echo 'Copyright check succeeded.'
    exit 0
fi
