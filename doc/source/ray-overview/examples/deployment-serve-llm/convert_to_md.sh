#!/bin/bash

set -exo pipefail

for nb in \
  "README" \
  "small-size-llm/README" \
  "medium-size-llm/README" \
  "large-size-llm/README" \
  "vision-llm/README" \
  "reasoning-llm/README" \
  "hybrid-reasoning-llm/README"
do
  jupyter nbconvert "${nb}.ipynb" --to markdown --output "README.md"
done
