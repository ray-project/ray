#!/usr/bin/env bash

set -exu

export DEBIAN_FRONTEND=noninteractive
export METADATA_FILE="/imagegeneration/metadatafile"
export HELPER_SCRIPTS="/imagegeneration/helpers"
export INSTALLER_SCRIPT_FOLDER="/imagegeneration/installers"
export BOOST_VERSIONS="1.69.0"
export BOOST_DEFAULT="1.69.0"

apt-get update -y

chmod 777 -R /imagegeneration
chmod 777 -R /etc/environment
chmod 775 -R /opt

chown -R agentadmin:root /imagegeneration/helpers
chown -R agentadmin:root /imagegeneration/installers

cp ./image/postgresql.sh /imagegeneration/installers/postgresql.sh

cp ./image/bazel.sh /imagegeneration/installers/bazel.sh

cp ./image/toolcache.json ${INSTALLER_SCRIPT_FOLDER}/toolcache.json

mkdir -p /etc/vsts
chmod 777 /etc/vsts

cat << EOF > /etc/vsts/machine_instance.conf
# Name of the pool supported by this image
POOL_NAME="RayPipelineAgentPoolStandardF16sv2"
EOF

source /imagegeneration/installers/postgresql.sh

# source /imagegeneration/installers/1604/powershellcore.sh

source /imagegeneration/installers/ruby.sh

source /imagegeneration/installers/rust.sh

source /imagegeneration/installers/sbt.sh

source /imagegeneration/installers/sphinx.sh

source /imagegeneration/installers/subversion.sh

source /imagegeneration/installers/terraform.sh

source /imagegeneration/installers/vcpkg.sh

source /imagegeneration/installers/zeit-now.sh

source /imagegeneration/installers/1604/android.sh

# source /imagegeneration/installers/1604/azpowershell.sh

source /imagegeneration/helpers/containercache.sh

source /imagegeneration/installers/python.sh

source /imagegeneration/installers/boost.sh

source /imagegeneration/installers/bazel.sh

sleep 30

# /usr/sbin/waagent -force -deprovision+user && export HISTSIZE=0 && sync

echo "Fix done!!!"
