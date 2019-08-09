#!/bin/bash

pkill -f cartpole_server.py
(python cartpole_server.py 2>&1 | grep -v 200) &
pid=$!

while ! curl localhost:9900; do   
  sleep 1
done

python cartpole_client.py --stop-at-reward=100
kill $pid
