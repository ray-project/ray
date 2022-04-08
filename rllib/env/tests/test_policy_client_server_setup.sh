#!/bin/bash

# Driver script for testing RLlib's client/server setup.
# Run as follows:
# $ test_policy_client_server_setup.sh [inference-mode: local|remote] [env: cartpole|cartpole-dummy-2-episodes|unity3d]

rm -f last_checkpoint.out

if [ "$1" == "local" ]; then
  inference_mode=local
else
  inference_mode=remote
fi

# CartPole client/server setup.
if [ "$2" == "cartpole" ]; then
  server_script=cartpole_server.py
  client_script=cartpole_client.py
  stop_criterion="--stop-reward=150.0"
  trainer_cls="PPO"
elif [ "$2" == "cartpole_lstm" ]; then
  server_script=cartpole_server.py
  client_script=cartpole_client.py
  stop_criterion="--stop-reward=150.0"
  trainer_cls="IMPALA --use-lstm"
# Unity3D dummy setup.
elif [ "$2" == "unity3d" ]; then
  server_script=unity3d_server.py
  client_script=unity3d_dummy_client.py
  stop_criterion="--num-episodes=10"
  trainer_cls="PPO"
# CartPole dummy test using 2 simultaneous episodes on the client.
# One episode has training_enabled=False (its data should NOT arrive at server).
else
  server_script=cartpole_server.py
  client_script=dummy_client_with_two_episodes.py
  stop_criterion="--dummy-arg=dummy"  # no stop criterion: client script terminates either way
  trainer_cls="PPO"
fi

pkill -f $server_script
sleep 1

if [ -f test_policy_client_server_setup.sh ]; then
    basedir="../../examples/serving"
else
    basedir="rllib/examples/serving"  # In bazel.
fi


# Start server with 2 workers (will listen on ports 9900 and 9901 for client
# connections).
# Do not attempt to restore from checkpoint; leads to errors on travis.
(python $basedir/$server_script --run=$trainer_cls --num-workers=2 --no-restore 2>&1 | grep -v 200) &
server_pid=$!

echo "Waiting for server to start ..."
while ! curl localhost:9900; do
  sleep 1
done
echo "Remote worker #1 on port 9900 is up!"
while ! curl localhost:9901; do
  sleep 1
done
echo "Remote worker #2 on port 9901 is up!"

# Start client 1 (connect to port 9900).
sleep 2
(python $basedir/$client_script --inference-mode=$inference_mode --port=9900) &
client1_pid=$!

# Start client 2 (connect to port 9901).
sleep 2
(python $basedir/$client_script --inference-mode=$inference_mode --port=9901) &
client2_pid=$!

# Start client 3 (also connecting to port 9901) and run it until it reaches
# x reward (CartPole) or n episodes (dummy Unity3D).
# Then stop everything.
sleep 2
python $basedir/$client_script --inference-mode=$inference_mode --port=9901 "$stop_criterion"

kill $server_pid $client1_pid $client2_pid || true
