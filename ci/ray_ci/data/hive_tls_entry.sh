#!/usr/bin/env bash
# Generate an ephemeral CA and server certificate inside the Hive container.
set -euo pipefail
umask 077
work=/tmp/ray-hive-tls
mkdir -p "$work/conf"

keytool -genkeypair -alias ca -dname CN=ray-hive-test-ca -keyalg RSA \
  -ext bc:c -validity 1 -keystore "$work/ca.p12" -storetype PKCS12 \
  -storepass:env HIVE_TLS_PASSWORD -noprompt
keytool -exportcert -rfc -alias ca -keystore "$work/ca.p12" \
  -storepass:env HIVE_TLS_PASSWORD -file "$work/ca.pem"
keytool -genkeypair -alias hive -dname CN=hiveserver2 -keyalg RSA \
  -validity 1 -keystore "$work/server.p12" -storetype PKCS12 \
  -storepass:env HIVE_TLS_PASSWORD -noprompt
keytool -certreq -alias hive -keystore "$work/server.p12" \
  -storepass:env HIVE_TLS_PASSWORD -file "$work/server.csr"
keytool -gencert -alias ca -keystore "$work/ca.p12" \
  -storepass:env HIVE_TLS_PASSWORD -infile "$work/server.csr" \
  -outfile "$work/server.pem" -rfc -ext SAN=dns:hiveserver2 -ext EKU=serverAuth
keytool -importcert -alias ca -keystore "$work/server.p12" \
  -storepass:env HIVE_TLS_PASSWORD -file "$work/ca.pem" -noprompt
keytool -importcert -alias hive -keystore "$work/server.p12" \
  -storepass:env HIVE_TLS_PASSWORD -file "$work/server.pem" -noprompt

# Preserve the image's Hive defaults before adding TLS settings.
: "${HIVE_WAREHOUSE_PATH:=/opt/hive/data/warehouse}"
export HIVE_WAREHOUSE_PATH
envsubst < "$HIVE_HOME/conf/hive-site.xml.template" | sed '$d' > "$work/conf/hive-site.xml"
cat >> "$work/conf/hive-site.xml" <<CONFIG
  <property><name>hive.server2.use.SSL</name><value>true</value></property>
  <property><name>hive.server2.keystore.path</name><value>$work/server.p12</value></property>
  <property><name>hive.server2.keystore.password</name><value>$HIVE_TLS_PASSWORD</value></property>
  <property><name>hive.server2.keystore.type</name><value>PKCS12</value></property>
</configuration>
CONFIG
export HIVE_CUSTOM_CONF_DIR="$work/conf"
exec /entrypoint.sh
