
# Launching Ray Clusters on AWS

This guide details the steps needed to start a Ray cluster on AWS.

To start an AWS Ray cluster, you need to use the Ray Cluster Launcher with AWS Python SDK.


## Install Ray Cluster Launcher
The Ray Cluster Launcher is part of the `ray` command line tool. It allows you to start, stop and attach to a running ray cluster using commands such as  `ray up`, `ray down` and `ray attach`. You can use pip to install the ray command line tool with cluster launcher support. Follow [install ray](https://docs.ray.io/en/latest/ray-overview/installation.html) for more detailed instructions.

```
# install ray
pip install -U ray[default]
```

## Install and Configure AWS Python SDK (Boto3)

Next, install AWS SDK using `pip install -U boto3` and configure your AWS credentials following [setup AWS credentials](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html).

```
# install AWS Python SDK (boto3)
pip install -U boto3

# setup AWS credentials using environment variables
export AWS_ACCESS_KEY_ID=foo
export AWS_SECRET_ACCESS_KEY=bar
export AWS_SESSION_TOKEN=baz

# alternatively, you can setup AWS credentials using ~/.aws/credentials file
echo "[default]
aws_access_key_id=foo
aws_secret_access_key=bar
aws_session_token=baz" >> ~/.aws/credentials
```

## Start Ray with the Ray Cluster Launcher

Once Boto3 is configured to manage resources in your AWS account, you should be ready to launch your cluster using the cluster launcher. The provided [example-full.yaml](https://github.com/ray-project/ray/tree/master/python/ray/autoscaler/aws/example-full.yaml) cluster config file will create a small cluster with an m5.large head node (on-demand) configured to autoscale up to two m5.large workers ([spot-instances](https://aws.amazon.com/ec2/spot/)).

Test that it works by running the following commands from your local machine:

```
# Download the example-full.yaml
wget https://raw.githubusercontent.com/ray-project/ray/master/python/ray/autoscaler/aws/example-full.yaml

# Create or update the cluster. When the command finishes, it will print
# out the command that can be used to SSH into the cluster head node.
ray up example-full.yaml

# Get a remote shell on the head node.
ray attach example-full.yaml

# Try running a Ray program.
python -c 'import ray; ray.init()'
exit

# Tear down the cluster.
ray down example-full.yaml
```

Congrats, you have started a Ray cluster on AWS!


If you want to learn more about Ray Cluster Launcher. See this blog post for a [step by step guide](https://medium.com/distributed-computing-with-ray/a-step-by-step-guide-to-scaling-your-first-python-application-in-the-cloud-8761fe331ef1) to using the Ray Cluster Launcher.


## AWS Configurations

### Using Amazon EFS

To utilize Amazon EFS in the Ray cluster, install some utilities and mount the EFS in `setup_commands`, as follows. Note that these instructions only work if you are using the Ray Cluster Launcher on AWS.

```
# Note You need to replace the {{FileSystemId}} to your own EFS ID before using the config.
# You may also need to set correct SecurityGroupIds for the instances in the config file.

setup_commands:
    - sudo kill -9 `sudo lsof /var/lib/dpkg/lock-frontend | awk '{print $2}' | tail -n 1`;
        sudo pkill -9 apt-get;
        sudo pkill -9 dpkg;
        sudo dpkg --configure -a;
        sudo apt-get -y install binutils;
        cd $HOME;
        git clone https://github.com/aws/efs-utils;
        cd $HOME/efs-utils;
        ./build-deb.sh;
        sudo apt-get -y install ./build/amazon-efs-utils*deb;
        cd $HOME;
        mkdir efs;
        sudo mount -t efs {{FileSystemId}}:/ efs;
        sudo chmod 777 efs;
```

### Accessing S3

In various scenarios, worker nodes may need write access to the S3 bucket. E.g. Ray Tune has the option that worker nodes write distributed checkpoints to S3 instead of syncing back to the driver using rsync.

If you see errors like “Unable to locate credentials”, make sure that the correct `IamInstanceProfile` is configured for worker nodes in `cluster.yaml` file. This may look like:

```
worker_nodes:
    InstanceType: m5.xlarge
    ImageId: latest_dlami
    IamInstanceProfile:
        Arn: arn:aws:iam::YOUR_AWS_ACCOUNT:YOUR_INSTANCE_PROFILE
```

You can verify if the set up is correct by entering one worker node and do

```
aws configure list
```

You should see something like

```
      Name                    Value             Type    Location
      ----                    -----             ----    --------
   profile                <not set>             None    None
access_key     ****************XXXX         iam-role
secret_key     ****************YYYY         iam-role
    region                <not set>             None    None
```

Please refer to this [discussion](https://github.com/ray-project/ray/issues/9327) for more details.