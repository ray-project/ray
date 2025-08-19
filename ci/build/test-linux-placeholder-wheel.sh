#!/bin/bash
set -exuo pipefail

PYTHON="$1"

if [[ ! "${OSTYPE}" =~ ^linux ]]; then
  echo "ERROR: This wheel test script is only for Linux platforms." >/dev/stderr
  exit 1
fi

# TODO (elliot-barn): list python versions
ls -d -- /opt/python/*/bin/

PYTHON_EXE="/opt/python/${PYTHON}/bin/python"
PIP_CMD="$(dirname "$PYTHON_EXE")/pip"

# Find the appropriate wheel by grepping for the Python version.
PYTHON_WHEEL=$(find ./.whl -maxdepth 1 -type f -name "*${PYTHON}*.whl" -print -quit)

if [[ -z "$PYTHON_WHEEL" ]]; then
  echo "No wheel found for pattern *${PYTHON}*.whl" >/dev/stderr
  exit 1
fi

# Print some env info
"$PYTHON_EXE" --version

# Update pip
"$PIP_CMD" install --upgrade pip

# Install the wheel.
"$PIP_CMD" uninstall -y ray
"$PIP_CMD" install --no-deps "$PYTHON_WHEEL" --use-pep517


# Check the wheel content
echo "📦 Checking metadata files in $PYTHON_WHEEL"

DIST=ray-3.0.0.dev0.dist-info

# Allowed files
ALLOWED=("${DIST}/entry_points.txt" "${DIST}/METADATA" "${DIST}/RECORD" "${DIST}/top_level.txt" "${DIST}/WHEEL")

# List files inside the wheel (without extracting)
FILES=$(unzip -Z1 "$PYTHON_WHEEL")

# Filter only allowed files (found anywhere in archive)
FOUND=()
for f in "${ALLOWED[@]}"; do
  if echo "$FILES" | grep -q "$f$"; then
    FOUND+=("$f")
  fi
done

echo "✅ Found expected files:"
printf ' - %s\n' "${FOUND[@]}"

# Check for unexpected files
# Build a regex pattern from allowed files (exact matches with ^...$)
ALLOWED_REGEX=$(printf '^%s$|' "${ALLOWED[@]}")
# Remove the trailing '|'
ALLOWED_REGEX=${ALLOWED_REGEX%|}

# Find files that do NOT match the allowed list
UNEXPECTED=$(echo "$FILES" | grep -Ev "$ALLOWED_REGEX" || true)


if [[ -n "$UNEXPECTED" ]]; then
  echo -e "\n⚠️ Unexpected files present in wheel:"
  echo "$UNEXPECTED"
else
  echo -e "\n🎉 No unexpected files found."
fi
