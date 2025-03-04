#!/usr/bin/env bash

apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends openjdk-8-jdk net-tools curl netcat gnupg libsnappy-dev && rm -rf /var/lib/apt/lists/*

export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64/

curl -O https://dist.apache.org/repos/dist/release/hadoop/common/KEYS

gpg --import KEYS

export HADOOP_VERSION=3.2.4
export HADOOP_URL=https://downloads.apache.org/hadoop/common/hadoop-$HADOOP_VERSION/hadoop-$HADOOP_VERSION.tar.gz

set -x && curl -fSL $HADOOP_URL -o /tmp/hadoop.tar.gz && curl -fSL $HADOOP_URL.asc -o /tmp/hadoop.tar.gz.asc && gpg --verify /tmp/hadoop.tar.gz.asc && tar -xvf /tmp/hadoop.tar.gz -C /opt/ && rm /tmp/hadoop.tar.gz*

ln -s /opt/hadoop-$HADOOP_VERSION/etc/hadoop /etc/hadoop

mkdir /opt/hadoop-$HADOOP_VERSION/logs

mkdir /hadoop-data

export HADOOP_HOME=/opt/hadoop-$HADOOP_VERSION
export HADOOP_CONF_DIR=/etc/hadoop

export USER=root
export PATH=$HADOOP_HOME/bin/:$PATH

export HDFS_DATANODE_USER=root
export HDFS_NAMENODE_USER=root
export HDFS_SECONDARYNAMENODE_USER=root

export YARN_NODEMANAGER_USER=root
export YARN_RESOURCEMANAGER_USER=root

# The following script is mainly to set up `/etc/hadoop/core-site.html`.
wget https://raw.githubusercontent.com/big-data-europe/docker-hadoop/master/base/entrypoint.sh
chmod a+x entrypoint.sh
./entrypoint.sh

# Add JAVA_HOME env var to `/etc/hadoop/hadoop-env.sh`
# Probably would be better to refer to JAVA_HOME env var, but not sure about sed syntax.
sed -i "1s/^/JAVA_HOME=\/usr\/lib\/jvm\/java-8-openjdk-amd64\/\n/" $HADOOP_CONF_DIR/hadoop-env.sh

# The following makes sure that ssh localhost should work without needing a password.
sudo apt-get update
sudo apt-get install -y openssh-server
sudo service ssh start
ssh-keygen -t rsa -N "" -f ~/.ssh/id_rsa
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
chmod 640 ~/.ssh/authorized_keys
sudo service ssh restart

# without this `jps` won't show NameNode but only SecondaryNameNode
yes | hadoop namenode -format
$HADOOP_HOME/sbin/start-all.sh

# Check that NameNode is up and running.
res=$(jps | grep -c NameNode)
if [[ $res == 2 ]]; then
  echo "NameNode is up and running."
else
  echo "Something is wrong with hdfs setup."
  exit 1
fi

hdfs dfs -mkdir /test

# Generate an env file to be used in `test_remote_storage_hdfs` unit test.
destdir=/tmp/hdfs_env
touch $destdir
for key in "JAVA_HOME" "HADOOP_HOME" "HADOOP_CONF_DIR" "USER"
do
  # use indirection to access a var by its name.
  echo "$key=${!key}" >> $destdir
done

# Needed for `test_remote_storage_hdfs` unit test to specify hdfs uri.
echo -e "CONTAINER_ID=$(hostname)\nHDFS_PORT=8020" >> $destdir

# Needed for pyarrow to work.
echo "CLASSPATH=$(hadoop classpath --glob)" >> $destdir
