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
