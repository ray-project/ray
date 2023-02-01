#!/bin/bash

curl -fSL -O "https://raw.githubusercontent.com/jcoffi/cluster-anywhere/master/docker/anywhere-tailscale/startup.sh" /home/ray/startup.sh
sudo chmod +x /home/ray/startup.sh
curl -fSL -O "https://raw.githubusercontent.com/jcoffi/cluster-anywhere/master/docker/anywhere-tailscale/run_tests.sh" /home/ray/run_tests.sh
sudo chmod +x /home/ray/run_tests.sh
./home/ray/run_tests.sh