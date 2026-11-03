#!/usr/bin/env bash
set -euo pipefail

export HIVE_CONF_DIR="${HIVE_HOME}/conf"
export HADOOP_CONF_DIR="$HIVE_CONF_DIR"
export KRB5_CONFIG=/krb/krb5.conf
export HADOOP_CLIENT_OPTS="-Xmx1G -Djava.security.krb5.conf=/krb/krb5.conf"
ln -sf /security/core-site.xml "$HIVE_CONF_DIR/core-site.xml"
ln -sf /security/hive-site.xml "$HIVE_CONF_DIR/hive-site.xml"
"$HIVE_HOME/bin/schematool" -dbType derby -initOrUpgradeSchema
exec "$HIVE_HOME/bin/hive" --skiphadoopversion --skiphbasecp --service hiveserver2
