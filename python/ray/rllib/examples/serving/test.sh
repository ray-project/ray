#!/bin/bash

(python cartpole_server.py 2>&1 | grep -v 200) &

while ! nc -z localhost 8900; do   
  sleep 0.1
done

python cartpole_client.py --stop-at-reward=100
