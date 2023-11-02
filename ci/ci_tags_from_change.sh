#!/bin/bash

exec python ci/pipeline/determine_tests_to_run.py --output rayci_tags
