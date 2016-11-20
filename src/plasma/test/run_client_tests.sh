./build/plasma_store -s /tmp/store1 &
./build/plasma_manager -m /tmp/manager1 -s /tmp/store1 -h 127.0.0.1 -p 11111 -r 127.0.0.1:6379 &
./build/plasma_store -s /tmp/store2 &
./build/plasma_manager -m /tmp/manager2 -s /tmp/store2 -h 127.0.0.1 -p 22222 -r 127.0.0.1:6379 &
../common/thirdparty/redis-3.2.3/src/redis-server &
sleep 1
./build/client_tests
kill %1
kill %2
kill %3
kill %4

