#!/usr/bin/env bash

set -e

anyscale job submit -f job.yaml --follow
