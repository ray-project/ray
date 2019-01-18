# Stop backend processes
ray stop
# Kill Java workers
ps aux | grep DefaultWorker | grep -v grep | awk '{print $2}' | xargs kill -9
# Remove temp files
rm -rf /tmp/ray
