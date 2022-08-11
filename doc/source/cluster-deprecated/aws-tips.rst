.. include:: we_are_hiring.rst

.. _aws-cluster:

AWS Configurations
-------------------

.. _aws-cluster-efs:

Using Amazon EFS
~~~~~~~~~~~~~~~~

To use Amazon EFS, install some utilities and mount the EFS in ``setup_commands``. Note that these instructions only work if you are using the AWS Autoscaler.

.. note::

  You need to replace the ``{{FileSystemId}}`` to your own EFS ID before using the config. You may also need to set correct ``SecurityGroupIds`` for the instances in the config file.

.. code-block:: yaml

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

.. _aws-cluster-s3:

Configure worker nodes to access Amazon S3
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

In various scenarios, worker nodes may need write access to the S3 bucket.
E.g. Ray Tune has the option that worker nodes write distributed checkpoints to S3 instead of syncing back to the driver using rsync.

If you see errors like "Unable to locate credentials", make sure that the correct ``IamInstanceProfile`` is configured for worker nodes in ``cluster.yaml`` file.
This may look like:

.. code-block:: text

 worker_nodes:
     InstanceType: m5.xlarge
     ImageId: latest_dlami
     IamInstanceProfile:
         Arn: arn:aws:iam::YOUR_AWS_ACCOUNT:YOUR_INSTANCE_PROFILE

You can verify if the set up is correct by entering one worker node and do

.. code-block:: bash

 aws configure list

You should see something like

.. code-block:: text

       Name                    Value             Type    Location
       ----                    -----             ----    --------
    profile                <not set>             None    None
 access_key     ****************XXXX         iam-role
 secret_key     ****************YYYY         iam-role
     region                <not set>             None    None

Please refer to `this discussion <https://github.com/ray-project/ray/issues/9327>`__ for more details.


.. _aws-cluster-cloudwatch:

Using Amazon CloudWatch
-----------------------

Amazon CloudWatch is a monitoring and observability service that provides data and actionable insights to monitor your applications, respond to system-wide performance changes, and optimize resource utilization.
CloudWatch integration with Ray requires an AMI (or Docker image) with the Unified CloudWatch Agent pre-installed.

AMIs with the Unified CloudWatch Agent pre-installed are provided by the Amazon Ray Team, and are currently available in the us-east-1, us-east-2, us-west-1, and us-west-2 regions.
Please direct any questions, comments, or issues to the `Amazon Ray Team <https://github.com/amzn/amazon-ray/issues/new/choose>`_.

The table below lists AMIs with the Unified CloudWatch Agent pre-installed in each region, and you can also find AMIs at `amazon-ray README <https://github.com/amzn/amazon-ray>`_.

.. list-table:: All available unified CloudWatch agent images

    * - Base AMI
      - AMI ID
      - Region
      - Unified CloudWatch Agent Version
    * - AWS Deep Learning AMI (Ubuntu 18.04, 64-bit)
      - ami-069f2811478f86c20
      - us-east-1
      - v1.247348.0b251302
    * - AWS Deep Learning AMI (Ubuntu 18.04, 64-bit)
      - ami-058cc0932940c2b8b
      - us-east-2
      - v1.247348.0b251302
    * - AWS Deep Learning AMI (Ubuntu 18.04, 64-bit)
      - ami-044f95c9ef12883ef
      - us-west-1
      - v1.247348.0b251302
    * - AWS Deep Learning AMI (Ubuntu 18.04, 64-bit)
      - ami-0d88d9cbe28fac870
      - us-west-2
      - v1.247348.0b251302

.. note::

    Using Amazon CloudWatch will incur charges, please refer to `CloudWatch pricing <https://aws.amazon.com/cloudwatch/pricing/>`_ for details.

Getting started
~~~~~~~~~~~~~~~

1. Create a minimal cluster config YAML named ``cloudwatch-basic.yaml`` with the following contents:
====================================================================================================

.. code-block:: yaml

    provider:
        type: aws
        region: us-west-2
        availability_zone: us-west-2a
        # Start by defining a `cloudwatch` section to enable CloudWatch integration with your Ray cluster.
        cloudwatch:
            agent:
                # Path to Unified CloudWatch Agent config file
                config: "cloudwatch/example-cloudwatch-agent-config.json"
            dashboard:
                # CloudWatch Dashboard name
                name: "example-dashboard-name"
                # Path to the CloudWatch Dashboard config file
                config: "cloudwatch/example-cloudwatch-dashboard-config.json"

    auth:
        ssh_user: ubuntu

    available_node_types:
        ray.head.default:
            node_config:
            InstanceType: c5a.large
            ImageId: ami-0d88d9cbe28fac870  # Unified CloudWatch agent pre-installed AMI, us-west-2
            resources: {}
        ray.worker.default:
            node_config:
                InstanceType: c5a.large
                ImageId: ami-0d88d9cbe28fac870  # Unified CloudWatch agent pre-installed AMI, us-west-2
                IamInstanceProfile:
                    Name: ray-autoscaler-cloudwatch-v1
            resources: {}
            min_workers: 0

2. Download CloudWatch Agent and Dashboard config.
==================================================

First, create a ``cloudwatch`` directory in the same directory as ``cloudwatch-basic.yaml``.
Then, download the example `CloudWatch Agent <https://github.com/ray-project/ray/blob/master/python/ray/autoscaler/aws/cloudwatch/example-cloudwatch-agent-config.json>`_ and `CloudWatch Dashboard <https://github.com/ray-project/ray/blob/master/python/ray/autoscaler/aws/cloudwatch/example-cloudwatch-dashboard-config.json>`_ config files to the ``cloudwatch`` directory.

.. code-block:: console

    $ mkdir cloudwatch
    $ cd cloudwatch
    $ wget https://raw.githubusercontent.com/ray-project/ray/master/python/ray/autoscaler/aws/cloudwatch/example-cloudwatch-agent-config.json
    $ wget https://raw.githubusercontent.com/ray-project/ray/master/python/ray/autoscaler/aws/cloudwatch/example-cloudwatch-dashboard-config.json

3. Run ``ray up cloudwatch-basic.yaml`` to start your Ray Cluster.
==================================================================

This will launch your Ray cluster in ``us-west-2`` by default. When launching a cluster for a different region, you'll need to change your cluster config YAML file's ``region`` AND ``ImageId``.
See the "Unified CloudWatch Agent Images" table above for available AMIs by region.

4. Check out your Ray cluster's logs, metrics, and dashboard in the `CloudWatch Console <https://console.aws.amazon.com/cloudwatch/>`_!
=======================================================================================================================================

A tail can be acquired on all logs written to a CloudWatch log group by ensuring that you have the `AWS CLI V2+ installed <https://docs.aws.amazon.com/cli/latest/userguide/install-cliv2.html>`_ and then running:

.. code-block:: bash

    aws logs tail $log_group_name --follow

Advanced Setup
~~~~~~~~~~~~~~

Refer to `example-cloudwatch.yaml <https://github.com/ray-project/ray/blob/master/python/ray/autoscaler/aws/example-cloudwatch.yaml>`_ for a complete example.

1. Choose an AMI with the Unified CloudWatch Agent pre-installed.
=================================================================

Ensure that you're launching your Ray EC2 cluster in the same region as the AMI,
then specify the ``ImageId`` to use with your cluster's head and worker nodes in your cluster config YAML file.

The following CLI command returns the latest available Unified CloudWatch Agent Image for ``us-west-2``:

.. code-block:: bash

    aws ec2 describe-images --region us-west-2 --filters "Name=owner-id,Values=160082703681" "Name=name,Values=*cloudwatch*" --query 'Images[*].[ImageId,CreationDate]' --output text | sort -k2 -r | head -n1

.. code-block:: yaml

    available_node_types:
        ray.head.default:
            node_config:
            InstanceType: c5a.large
            ImageId: ami-0d88d9cbe28fac870
        ray.worker.default:
            node_config:
            InstanceType: c5a.large
            ImageId: ami-0d88d9cbe28fac870

To build your own AMI with the Unified CloudWatch Agent installed:

1. Follow the `CloudWatch Agent Installation <https://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/install-CloudWatch-Agent-on-EC2-Instance.html>`_ user guide to install the Unified CloudWatch Agent on an EC2 instance.
2. Follow the `EC2 AMI Creation <https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/AMIs.html#creating-an-ami>`_ user guide to create an AMI from this EC2 instance.

2. Define your own CloudWatch Agent, Dashboard, and Alarm JSON config files.
============================================================================

You can start by using the example `CloudWatch Agent <https://github.com/ray-project/ray/blob/master/python/ray/autoscaler/aws/cloudwatch/example-cloudwatch-agent-config.json>`_, `CloudWatch Dashboard <https://github.com/ray-project/ray/blob/master/python/ray/autoscaler/aws/cloudwatch/example-cloudwatch-dashboard-config.json>`_ and `CloudWatch Alarm <https://github.com/ray-project/ray/blob/master/python/ray/autoscaler/aws/cloudwatch/example-cloudwatch-alarm-config.json>`_ config files.

These example config files include the following features:

**Logs and Metrics**:  Logs written to ``/tmp/ray/session_*/logs/**.out`` will be available in the ``{cluster_name}-ray_logs_out`` log group,
and logs written to ``/tmp/ray/session_*/logs/**.err`` will be available in the ``{cluster_name}-ray_logs_err`` log group.
Log streams are named after the EC2 instance ID that emitted their logs.
Extended EC2 metrics including CPU/Disk/Memory usage and process statistics can be found in the ``{cluster_name}-ray-CWAgent`` metric namespace.

**Dashboard**: You will have a cluster-level dashboard showing total cluster CPUs and available object store memory.
Process counts, disk usage, memory usage, and CPU utilization will be displayed as both cluster-level sums and single-node maximums/averages.

**Alarms**: Node-level alarms tracking prolonged high memory, disk, and CPU usage are configured. Alarm actions are NOT set,
and must be manually provided in your alarm config file.

For more advanced options, see the `Agent <https://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/CloudWatch-Agent-Configuration-File-Details.html>`_, `Dashboard <https://docs.aws.amazon.com/AmazonCloudWatch/latest/APIReference/CloudWatch-Dashboard-Body-Structure.html>`_ and `Alarm <https://docs.aws.amazon.com/AmazonCloudWatch/latest/APIReference/API_PutMetricAlarm.html>`_ config user guides.

CloudWatch Agent, Dashboard, and Alarm JSON config files support the following variables:

``{instance_id}``: Replaced with each EC2 instance ID in your Ray cluster.

``{region}``: Replaced with your Ray cluster's region.

``{cluster_name}``: Replaced with your Ray cluster name.

See CloudWatch Agent `Configuration File Details <https://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/CloudWatch-Agent-Configuration-File-Details.html>`_ for additional variables supported natively by the Unified CloudWatch Agent.

.. note::
    Remember to replace the ``AlarmActions`` placeholder in your CloudWatch Alarm config file!

.. code-block:: json

     "AlarmActions":[
         "TODO: Add alarm actions! See https://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/AlarmThatSendsEmail.html"
      ]

3. Reference your CloudWatch JSON config files in your cluster config YAML.
===========================================================================

Specify the file path to your CloudWatch JSON config files relative to the working directory that you will run ``ray up`` from:

.. code-block:: yaml

     provider:
        cloudwatch:
            agent:
                config: "cloudwatch/example-cloudwatch-agent-config.json"


4. Set your IAM Role and EC2 Instance Profile.
==============================================

By default the ``ray-autoscaler-cloudwatch-v1`` IAM role and EC2 instance profile is created at Ray cluster launch time.
This role contains all additional permissions required to integrate CloudWatch with Ray, namely the ``CloudWatchAgentAdminPolicy``, ``AmazonSSMManagedInstanceCore``, ``ssm:SendCommand``, ``ssm:ListCommandInvocations``, and ``iam:PassRole`` managed policies.

Ensure that all worker nodes are configured to use the ``ray-autoscaler-cloudwatch-v1`` EC2 instance profile in your cluster config YAML:

.. code-block:: yaml

    ray.worker.default:
        node_config:
            InstanceType: c5a.large
            IamInstanceProfile:
                Name: ray-autoscaler-cloudwatch-v1

5. Export Ray system metrics to CloudWatch.
===========================================

To export Ray's Prometheus system metrics to CloudWatch, first ensure that your cluster has the
Ray Dashboard installed, then uncomment the ``head_setup_commands`` section in `example-cloudwatch.yaml file <https://github.com/ray-project/ray/blob/master/python/ray/autoscaler/aws/example-cloudwatch.yaml>`_ file.
You can find Ray Prometheus metrics in the ``{cluster_name}-ray-prometheus`` metric namespace.

.. code-block:: yaml

    head_setup_commands:
  # Make `ray_prometheus_waiter.sh` executable.
  - >-
    RAY_INSTALL_DIR=`pip show ray | grep -Po "(?<=Location:).*"`
    && sudo chmod +x $RAY_INSTALL_DIR/ray/autoscaler/aws/cloudwatch/ray_prometheus_waiter.sh
  # Copy `prometheus.yml` to Unified CloudWatch Agent folder
  - >-
    RAY_INSTALL_DIR=`pip show ray | grep -Po "(?<=Location:).*"`
    && sudo cp -f $RAY_INSTALL_DIR/ray/autoscaler/aws/cloudwatch/prometheus.yml /opt/aws/amazon-cloudwatch-agent/etc
  # First get current cluster name, then let the Unified CloudWatch Agent restart and use `AmazonCloudWatch-ray_agent_config_{cluster_name}` parameter at SSM Parameter Store.
  - >-
    nohup sudo sh -c "`pip show ray | grep -Po "(?<=Location:).*"`/ray/autoscaler/aws/cloudwatch/ray_prometheus_waiter.sh
    `cat ~/ray_bootstrap_config.yaml | jq '.cluster_name'`
    >> '/opt/aws/amazon-cloudwatch-agent/logs/ray_prometheus_waiter.out' 2>> '/opt/aws/amazon-cloudwatch-agent/logs/ray_prometheus_waiter.err'" &

6. Update CloudWatch Agent, Dashboard and Alarm config files.
=============================================================

You can apply changes to the CloudWatch Logs, Metrics, Dashboard, and Alarms for your cluster by simply modifying the CloudWatch config files referenced by your Ray cluster config YAML and re-running ``ray up example-cloudwatch.yaml``.
The Unified CloudWatch Agent will be automatically restarted on all cluster nodes, and your config changes will be applied.

