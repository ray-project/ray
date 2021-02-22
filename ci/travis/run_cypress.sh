#!/usr/bin/env bash

ROOT="$(git rev-parse --show-toplevel)"
builtin cd "$ROOT" || exit 1

sudo npm install cypress
ray stop --force
ray start --head --dashboard-port=8653
cypress run --project dashboard/tests --headless
ray stop --force