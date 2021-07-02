#!/bin/bash

rm -f last_checkpoint.out
pkill -f cartpole_server.py
sleep 1

if [ -f test_local_inference.sh ]; then
    basedir="../../examples/serving"
else
    basedir="rllib/examples/serving"  # In bazel.
fi

# Start server with 2 workers (will listen on ports 9900 and 9901 for client
# connections).
# Do not attempt to restore from checkpoint; leads to errors on travis.
(python $basedir/cartpole_server.py --run=PPO --num-workers=2 --no-restore 2>&1 | grep -v 200) &
server_pid=$!

echo "Waiting for server to start"
while ! curl localhost:9900; do
  sleep 1
done
while ! curl localhost:9901; do
  sleep 1
done

# Start client 1 (port 9900).
sleep 2
(python $basedir/cartpole_client.py --inference-mode=local --port=9900) &
client1_pid=$!

# Start client 2 (port 9901).
sleep 2
(python $basedir/cartpole_client.py --inference-mode=local --port=9901) &
client2_pid=$!

# Start client 3 (also port 9901) and run it until it reaches 150.0
# reward. Then stop everything.
sleep 2
python $basedir/cartpole_client.py --stop-reward=150.0 --inference-mode=local --port=9901

kill $server_pid $client1_pid $client2_pid || true
