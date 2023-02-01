#!/bin/bash

curl -fSL -O "https://raw.githubusercontent.com/jcoffi/cluster-anywhere/master/docker/anywhere-tailscale/startup.sh"
sudo chmod +x /home/ray/startup.sh
curl -fSL -O "https://raw.githubusercontent.com/jcoffi/cluster-anywhere/master/docker/anywhere-tailscale/run_tests.sh"
sudo chmod +x /home/ray/run_tests.sh