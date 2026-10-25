#!/bin/bash

set -exo pipefail

pip3 install --no-cache-dir transformers==4.48.0 datasets==2.21.0
pip3 install --no-cache-dir --no-build-isolation deepspeed==0.18.9
