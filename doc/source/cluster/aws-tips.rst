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