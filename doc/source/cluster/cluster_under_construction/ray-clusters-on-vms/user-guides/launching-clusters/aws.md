
# Launching Ray Clusters on AWS

This guide details the steps needed to start a Ray cluster in AWS.

To start an AWS Ray cluster, you need to use Ray cluster launcher and AWS Python SDK.


## Install Ray Cluster Launcher
The Ray cluster launcher is part of the `ray` command line tool. It allows you to start, stop and attach to a running ray cluster using commands such as  `ray up`, `ray down` and `ray attach`. You can use pip to install it, or follow [install ray](https://docs.ray.io/en/latest/ray-overview/installation.html) for more detailed instructions.

```
# install ray cluster launcher, which is part of ray command line tool.
pip install ray
```

## Install and Configure AWS Python SDK (Boto3)

Next, install AWS SDK using `pip install boto3` and configure your AWS credentials following [setup AWS credentials](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html).

```
# install AWS Python SDK (boto3)
pip install boto3

# setup AWS credentials using environment variables
export AWS_ACCESS_KEY_ID=foo
export AWS_SECRET_ACCESS_KEY=bar
export AWS_SESSION_TOKEN=baz

# alternative, you can also setup AWS credentials using ~/.aws/credentials file
echo "[default]
aws_access_key_id=foo
aws_secret_access_key=bar
aws_session_token=baz" >> ~/.aws/credentials
```

## Start Ray with Ray Cluster Launcher

Once Boto3 is configured to manage resources on your AWS account, you should be ready to launch your cluster using cluster launcher. The provided [example-full.yaml](https://github.com/ray-project/ray/tree/master/python/ray/autoscaler/aws/example-full.yaml) cluster config file will create a small cluster with an m5.large head node (on-demand) configured to autoscale up to two m5.large workers ([spot-instances](https://aws.amazon.com/ec2/spot/)).

Test that it works by running the following commands from your local machine:

```
# Download the example-full.yaml
wget https://raw.githubusercontent.com/ray-project/ray/master/python/ray/autoscaler/aws/example-full.yaml

# Create or update the cluster. When the command finishes, it will print
# out the command that can be used to SSH into the cluster head node.
ray up example-full.yaml

# Get a remote screen on the head node.
ray attach example-full.yaml

# Try running a Ray program.
python -c 'import ray; ray.init()'
exit

# Tear down the cluster.
ray down example-full.yaml
```

Congrats, you have started a Ray cluster on AWS!


If you want to learn more about Ray cluster launcher. See this blog post for a [step by step guide](https://medium.com/distributed-computing-with-ray/a-step-by-step-guide-to-scaling-your-first-python-application-in-the-cloud-8761fe331ef1) to using the Ray Cluster Launcher.

