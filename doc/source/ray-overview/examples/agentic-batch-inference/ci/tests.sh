#!/bin/bash

set -euxo pipefail

python content/agentic_batch_parallel.py
python content/agentic_batch_conditional_routing.py
python content/agentic_batch_multi_turn_rollout.py