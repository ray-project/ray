pkill -9 python
pkill -9 local_scheduler
pkill -9 plasma_manager
pkill -9 plasma_store
pkill -9 global_scheduler
pkill -9 redis-server
pkill -9 redis
pkill -9 raylet
ps aux | grep ray | awk '{system("kill "$2);}'
