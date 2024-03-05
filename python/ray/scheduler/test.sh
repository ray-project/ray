SERVER_IP=ec2-34-228-78-134.compute-1.amazonaws.com:8000
ray stop --force && ray start --head
python /home/ubuntu/ray/python/ray/scheduler/init.py
python /home/ubuntu/ray/python/ray/scheduler/test.py

curl $SERVER_IP/render
curl $SERVER_IP/get/node-info
