./src/common/thirdparty/redis/src/redis-server --loglevel warning --loadmodule ./src/common/redis_module/libray_redis_module.so &
sleep 1s
valgrind --leak-check=full --error-exitcode=1 ./src/common/common_tests
valgrind --leak-check=full --error-exitcode=1 ./src/common/db_tests
valgrind --leak-check=full --error-exitcode=1 ./src/common/io_tests
valgrind --leak-check=full --error-exitcode=1 ./src/common/task_tests
valgrind --leak-check=full --error-exitcode=1 ./src/common/redis_tests
valgrind --leak-check=full --error-exitcode=1 ./src/common/task_table_tests
valgrind --leak-check=full --error-exitcode=1 ./src/common/object_table_tests
./src/common/thirdparty/redis/src/redis-cli shutdown
