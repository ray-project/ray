SERVER_IP=ec2-3-128-201-62.us-east-2.compute.amazonaws.com:8000
ray stop --force && ray start --head
python /home/ubuntu/ray/python/ray/scheduler/init.py
python /home/ubuntu/ray/python/ray/scheduler/test.py

curl $SERVER_IP/render
curl $SERVER_IP/get/node-info
