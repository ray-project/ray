#!/usr/bin/env bash

set -x

# Cause the script to exit if a single command fails.
set -e

#valgrind --track-origins=yes --leak-check=full --show-leak-kinds=all --leak-check-heuristics=stdstring \
#./streaming/src/streaming_channel_id_generator_tests
#
#valgrind --track-origins=yes --leak-check=full --show-leak-kinds=all --leak-check-heuristics=stdstring \
#./streaming/src/streaming_message_ring_buffer_tests
#
#valgrind --track-origins=yes --leak-check=full --show-leak-kinds=all --leak-check-heuristics=stdstring \
#./streaming/src/streaming_message_serialization_tests

./src/plasma/plasma_store_server -s /tmp/store1 -m 1000000000 &
pid=$!
sleep 1

#./streaming/src/streaming_writer_tests
#./streaming/src/streaming_reader_tests

valgrind --track-origins=yes --leak-check=full --show-leak-kinds=all --leak-check-heuristics=stdstring --error-exitcode=1 \
./streaming/src/streaming_reader_tests

kill $pid
wait $pid
