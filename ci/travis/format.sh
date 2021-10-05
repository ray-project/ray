#!/usr/bin/env bash
# YAPF + Clang formatter (if installed). This script formats all changed files from the last mergebase.
# You are encouraged to run this locally before pushing changes for review.

# Cause the script to exit if a single command fails
set -euo pipefail

printWarning() {
    printf '\033[31mLINT WARNING:\033[0m %s\n' "$@"
}

printInfo() {
    printf '\033[34mLINT:\033[0m %s\n' "$@"
}

if ! git diff --quiet &>/dev/null; then
    printWarning 'Found unstaged changes before lint. They will be mixed with reformatting, if there is any.'
fi

FLAKE8_VERSION_REQUIRED="3.9.1"
YAPF_VERSION_REQUIRED="0.23.0"
SHELLCHECK_VERSION_REQUIRED="0.7.1"
MYPY_VERSION_REQUIRED="0.782"

check_command_exist() {
    VERSION=""
    case "$1" in
        yapf)
            VERSION=$YAPF_VERSION_REQUIRED
            ;;
        flake8)
            VERSION=$FLAKE8_VERSION_REQUIRED
            ;;
        shellcheck)
            VERSION=$SHELLCHECK_VERSION_REQUIRED
            ;;
        mypy)
            VERSION=$MYPY_VERSION_REQUIRED
            ;;
        *)
            printWarning "$1 is not a required dependency"
            exit 1
    esac
    if ! [ -x "$(command -v "$1")" ]; then
        printWarning "$1 not installed. pip install $1==$VERSION"
        exit 1
    fi
}

check_command_exist yapf
check_command_exist flake8
check_command_exist mypy

# this stops git rev-parse from failing if we run this from the .git directory
builtin cd "$(dirname "${BASH_SOURCE:-$0}")"

ROOT="$(git rev-parse --show-toplevel)"
builtin cd "$ROOT" || exit 1

FLAKE8_VERSION=$(flake8 --version | head -n 1 | awk '{print $1}')
YAPF_VERSION=$(yapf --version | awk '{print $2}')
SHELLCHECK_VERSION=$(shellcheck --version | awk '/^version:/ {print $2}')
MYPY_VERSION=$(mypy --version | awk '{print $2}')
GOOGLE_JAVA_FORMAT_JAR=/tmp/google-java-format-1.7-all-deps.jar

# params: tool name, tool version, required version
tool_version_check() {
    if [ "$2" != "$3" ]; then
        printWarning "Ray uses $1 $3, You currently are using $2. This might generate different results."
    fi
}

# params: tool name, version command, desired version
tool_version_cmd_check() {
    local tool_version
    tool_version="$($2)"
    if [[ ! "$tool_version" =~ $3 ]]; then
        printWarning "Ray uses $1 $3, but running $2 indicates otherwise. This might generate different results."
    fi
}

tool_version_check "flake8" "$FLAKE8_VERSION" "$FLAKE8_VERSION_REQUIRED"
tool_version_check "yapf" "$YAPF_VERSION" "$YAPF_VERSION_REQUIRED"
tool_version_check "shellcheck" "$SHELLCHECK_VERSION" "$SHELLCHECK_VERSION_REQUIRED"
tool_version_check "mypy" "$MYPY_VERSION" "$MYPY_VERSION_REQUIRED"

if command -v clang-format >/dev/null; then
  tool_version_cmd_check "clang-format" "clang-format --version" "12.0"
else
    printWarning "clang-format 12 is not installed!"
    printWarning "To install on MacOS: brew install clang-format"
    printWarning "To install on Ubuntu: use package manager e.g. sudo apt install clang-format-12 -y && sudo update-alternatives --install /usr/bin/clang-format clang-format /usr/bin/clang-format-12 100"
    printWarning "Or run scripts/install-llvm-binaries.sh"
fi

if command -v clang-tidy >/dev/null; then
  tool_version_cmd_check "clang-tidy" "clang-tidy --version" "12.0"
else
    printWarning "clang-tidy 12 is not installed!"
    printWarning "To install on MacOS: brew install clang-tidy"
    printWarning "To install on Ubuntu: use package manager e.g. sudo apt install clang-tidy-12 -y && sudo update-alternatives --install /usr/bin/clang-tidy clang-tidy /usr/bin/clang-tidy-12 100"
    printWarning "Or run scripts/install-llvm-binaries.sh"
fi

if ! command -v clang >/dev/null; then
    printWarning "clang is not installed! clang-tidy will be disabled."
    printWarning "To install, use package manager, run scripts/install-llvm-binaries.sh (Ubuntu only), or compile from source."
fi

if command -v java >/dev/null; then
  if [ ! -f "$GOOGLE_JAVA_FORMAT_JAR" ]; then
    printWarning "Java code format tool google-java-format.jar is not installed, start to install it."
    wget https://github.com/google/google-java-format/releases/download/google-java-format-1.7/google-java-format-1.7-all-deps.jar -O "$GOOGLE_JAVA_FORMAT_JAR"
  fi
else
    printWarning "Java is not installed, skip formatting Java files!"
fi

if [[ $(flake8 --version) != *"flake8_quotes"* ]]; then
    printWarning "Ray uses flake8 with flake8_quotes. Might error without it. Install with: pip install flake8-quotes"
fi

if [[ $(flake8 --version) != *"flake8-bugbear"* ]]; then
    echo "WARNING: Ray uses flake8 with flake8-bugbear. Might error without it. Install with: pip install flake8-bugbear"
fi

SHELLCHECK_FLAGS=(
  "--exclude=1090"  # "Can't follow non-constant source. Use a directive to specify location."
  "--exclude=1091"  # "Not following {file} due to some error"
  "--exclude=2207"  # "Prefer mapfile or read -a to split command output (or quote to avoid splitting)." -- these aren't compatible with macOS's old Bash
)

YAPF_FLAGS=(
    '--style' "$ROOT/.style.yapf"
    '--recursive'
    '--parallel'
)

# TODO(dmitri): When more of the codebase is typed properly, the mypy flags
# should be set to do a more stringent check.
MYPY_FLAGS=(
    '--follow-imports=skip'
    '--ignore-missing-imports'
)

MYPY_FILES=(
    # Relative to python/ray
    'autoscaler/node_provider.py'
    'autoscaler/sdk.py'
    'autoscaler/_private/commands.py'
    # TODO(dmitri) Fails with meaningless error, maybe due to a bug in the mypy version
    # in the CI. Type check once we get serious about type checking:
    #'ray_operator/operator.py'
    'ray_operator/operator_utils.py'
)

YAPF_EXCLUDES=(
    '--exclude' 'python/ray/cloudpickle/*'
    '--exclude' 'python/build/*'
    '--exclude' 'python/ray/core/src/ray/gcs/*'
    '--exclude' 'python/ray/thirdparty_files/*'
    '--exclude' 'python/ray/_private/thirdparty/*'
)

GIT_LS_EXCLUDES=(
  ':(exclude)python/ray/cloudpickle/'
)

JAVA_EXCLUDES=(
  'java/api/src/main/java/io/ray/api/ActorCall.java'
  'java/api/src/main/java/io/ray/api/PyActorCall.java'
  'java/api/src/main/java/io/ray/api/RayCall.java'
)

JAVA_EXCLUDES_REGEX=""
for f in "${JAVA_EXCLUDES[@]}"; do
  JAVA_EXCLUDES_REGEX="$JAVA_EXCLUDES_REGEX|(${f//\//\/})"
done
JAVA_EXCLUDES_REGEX=${JAVA_EXCLUDES_REGEX#|}

# TODO(barakmich): This should be cleaned up. I've at least excised the copies
# of these arguments to this location, but the long-term answer is to actually
# make a flake8 config file
FLAKE8_PYX_IGNORES="--ignore=C408,E121,E123,E126,E211,E225,E226,E227,E24,E704,E999,W503,W504,W605"

shellcheck_scripts() {
  shellcheck "${SHELLCHECK_FLAGS[@]}" "$@"
}

# Runs mypy on each argument in sequence. This is different than running mypy
# once on the list of arguments.
mypy_on_each() {
    pushd python/ray
    for file in "$@"; do
       printInfo "Running mypy on $file"
       mypy ${MYPY_FLAGS[@]+"${MYPY_FLAGS[@]}"} "$file"
    done
    popd
}

# Format specified files
format_files() {
    local shell_files=() python_files=() bazel_files=()

    local name
    for name in "$@"; do
      local base="${name%.*}"
      local suffix="${name#${base}}"

      local shebang=""
      read -r shebang < "${name}" || true
      case "${shebang}" in
        '#!'*)
          shebang="${shebang#/usr/bin/env }"
          shebang="${shebang%% *}"
          shebang="${shebang##*/}"
          ;;
      esac

      if [ "${base}" = "WORKSPACE" ] || [ "${base}" = "BUILD" ] || [ "${suffix}" = ".BUILD" ] || [ "${suffix}" = ".bazel" ] || [ "${suffix}" = ".bzl" ]; then
        bazel_files+=("${name}")
      elif [ -z "${suffix}" ] && [ "${shebang}" != "${shebang#python}" ] || [ "${suffix}" != "${suffix#.py}" ]; then
        python_files+=("${name}")
      elif [ -z "${suffix}" ] && [ "${shebang}" != "${shebang%sh}" ] || [ "${suffix}" != "${suffix#.sh}" ]; then
        shell_files+=("${name}")
      else
        printWarning "failed to determine file type: ${name}" 1>&2
        return 1
      fi
    done

    if [ 0 -lt "${#python_files[@]}" ]; then
      yapf --in-place "${YAPF_FLAGS[@]}" -- "${python_files[@]}"
    fi

    if shellcheck --shell=sh --format=diff - < /dev/null; then
      if [ 0 -lt "${#shell_files[@]}" ]; then
        local difference
        difference="$(shellcheck_scripts --format=diff "${shell_files[@]}" || true && printf "-")"
        difference="${difference%-}"
        printf "%s" "${difference}" | patch -p1
      fi
    else
      printWarning "this version of shellcheck does not support diffs"
    fi
}

format_all_scripts() {
    command -v flake8 &> /dev/null;
    HAS_FLAKE8=$?

    printInfo "$(date)" "YAPF...."
    git ls-files -- '*.py' "${GIT_LS_EXCLUDES[@]}" | xargs -P 10 \
      yapf --in-place "${YAPF_EXCLUDES[@]}" "${YAPF_FLAGS[@]}"
    printInfo "$(date)" "MYPY...."
    mypy_on_each "${MYPY_FILES[@]}"
    if [ $HAS_FLAKE8 ]; then
      printInfo "$(date)" "Flake8...."
      git ls-files -- '*.py' "${GIT_LS_EXCLUDES[@]}" | xargs -P 5 \
        flake8 --config=.flake8

      git ls-files -- '*.pyx' '*.pxd' '*.pxi' "${GIT_LS_EXCLUDES[@]}" | xargs -P 5 \
        flake8 --config=.flake8 "$FLAKE8_PYX_IGNORES"
    fi

    if command -v shellcheck >/dev/null; then
      local shell_files non_shell_files
      non_shell_files=($(git ls-files -- ':(exclude)*.sh'))
      shell_files=($(git ls-files -- '*.sh'))
      if [ 0 -lt "${#non_shell_files[@]}" ]; then
        shell_files+=($(git --no-pager grep -l -- '^#!\(/usr\)\?/bin/\(env \+\)\?\(ba\)\?sh' "${non_shell_files[@]}" || true))
      fi
      if [ 0 -lt "${#shell_files[@]}" ]; then
        printInfo "$(date)" "shellcheck scripts...."
        shellcheck_scripts "${shell_files[@]}"
      fi
    fi
}

# Format all files, and print the diff to stdout for travis.
# Mypy is run only on files specified in the array MYPY_FILES.
format_all() {
    format_all_scripts "${@}"

    printInfo "$(date)" "clang-format...."
    if command -v clang-format >/dev/null; then
      git ls-files -- '*.cc' '*.h' '*.proto' "${GIT_LS_EXCLUDES[@]}" | xargs -P 5 clang-format -i
    fi

    printInfo "$(date)" "format java...."
    if command -v java >/dev/null & [ -f "$GOOGLE_JAVA_FORMAT_JAR" ]; then
      git ls-files -- '*.java' "${GIT_LS_EXCLUDES[@]}" | sed -E "\:$JAVA_EXCLUDES_REGEX:d" | xargs -P 5 java -jar "$GOOGLE_JAVA_FORMAT_JAR" -i
    fi

    printInfo "$(date)" "done!"
}

# Format files that differ from main branch. Ignores dirs that are not slated
# for autoformat yet.
format_changed() {
    # The `if` guard ensures that the list of filenames is not empty, which
    # could cause yapf to receive 0 positional arguments, making it hang
    # waiting for STDIN.
    #
    # `diff-filter=ACRM` and $MERGEBASE is to ensure we only format files that
    # exist on both branches.
    MERGEBASE="$(git merge-base upstream/master HEAD)"

    printInfo "Running fake8 on *.py ..."
    if ! git diff --diff-filter=ACRM --quiet --exit-code "$MERGEBASE" -- '*.py' &>/dev/null; then
        git diff --name-only --diff-filter=ACRM "$MERGEBASE" -- '*.py' | xargs -P 5 \
             yapf --in-place "${YAPF_EXCLUDES[@]}" "${YAPF_FLAGS[@]}"
        if command -v flake8 >/dev/null; then
            git diff --name-only --diff-filter=ACRM "$MERGEBASE" -- '*.py' | xargs -P 5 \
                 flake8 --config=.flake8
        fi
    fi

    printInfo "Running fake8 on cython files ..."
    if ! git diff --diff-filter=ACRM --quiet --exit-code "$MERGEBASE" -- '*.pyx' '*.pxd' '*.pxi' &>/dev/null; then
        if command -v flake8 >/dev/null; then
            git diff --name-only --diff-filter=ACRM "$MERGEBASE" -- '*.pyx' '*.pxd' '*.pxi' | xargs -P 5 \
                 flake8 --config=.flake8 "$FLAKE8_PYX_IGNORES"
        fi
    fi

    printInfo "Running clang-format ..."
    if command -v clang-format >/dev/null; then
        ci/travis/check-git-clang-format-output.sh
    fi

    printInfo "Running clang-tidy ..."
    if command -v clang-tidy >/dev/null && command -v clang >/dev/null; then
        ci/travis/check-git-clang-tidy-output.sh
    fi

    printInfo "Running Java format ..."
    if command -v java >/dev/null & [ -f "$GOOGLE_JAVA_FORMAT_JAR" ]; then
       if ! git diff --diff-filter=ACRM --quiet --exit-code "$MERGEBASE" -- '*.java' &>/dev/null; then
            git diff --name-only --diff-filter=ACRM "$MERGEBASE" -- '*.java' | sed -E "\:$JAVA_EXCLUDES_REGEX:d" | xargs -P 5 java -jar "$GOOGLE_JAVA_FORMAT_JAR" -i
        fi
    fi

    printInfo "Running shellcheck ..."
    if command -v shellcheck >/dev/null; then
        local shell_files non_shell_files
        non_shell_files=($(git diff --name-only --diff-filter=ACRM "$MERGEBASE" -- ':(exclude)*.sh'))
        shell_files=($(git diff --name-only --diff-filter=ACRM "$MERGEBASE" -- '*.sh'))
        if [ 0 -lt "${#non_shell_files[@]}" ]; then
            shell_files+=($(git --no-pager grep -l -- '^#!\(/usr\)\?/bin/\(env \+\)\?\(ba\)\?sh' "${non_shell_files[@]}" || true))
        fi
        if [ 0 -lt "${#shell_files[@]}" ]; then
            shellcheck_scripts "${shell_files[@]}"
        fi
    fi
}


# This flag formats individual files. --files *must* be the first command line
# arg to use this option.
if [ "${1-}" == '--files' ]; then
    format_files "${@:2}"
# If `--all` or `--scripts` are passed, then any further arguments are ignored.
# Format the entire python directory and other scripts.
elif [ "${1-}" == '--all-scripts' ]; then
    format_all_scripts "${@}"
    if [ -n "${FORMAT_SH_PRINT_DIFF-}" ]; then git --no-pager diff; fi
# Format the all Python, C++, Java and other script files.
elif [ "${1-}" == '--all' ]; then
    format_all "${@}"
    if [ -n "${FORMAT_SH_PRINT_DIFF-}" ]; then git --no-pager diff; fi
else
    # Add the upstream remote if it doesn't exist
    if ! git remote -v | grep -q upstream; then
        git remote add 'upstream' 'https://github.com/ray-project/ray.git'
    fi

    # Only fetch master since that's the branch we're diffing against.
    git fetch --quiet upstream master || true

    # Format only the files that changed in last commit.
    format_changed
fi

# Ensure import ordering
# Make sure that for every import psutil; import setproctitle
# There's a import ray above it.

PYTHON_EXECUTABLE=${PYTHON_EXECUTABLE:-python}

$PYTHON_EXECUTABLE ci/travis/check_import_order.py . -s ci -s python/ray/thirdparty_files -s python/build -s lib

if ! git diff --quiet &>/dev/null; then
    printWarning 'Found unstaged changes after lint, possibly from reformatting:'

    git --no-pager diff --name-only

    exit 1
fi
