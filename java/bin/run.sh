#!/bin/bash
bin=`dirname ${0}`
bin=`cd ${bin}; pwd`
basedir=${bin}/..
LIB_DIR=${basedir}/lib
LOG_DIR=${basedir}/logs
CONF_DIR=${basedir}/conf

cd ${basedir}

if [ ! -d ${LOG_DIR} ]; then
  mkdir -p ${LOG_DIR}
fi

# start ray head, including redis-server, object-store and raylet
# or just start node, means object-store, raylet
# usage:
#   - run.sh start --config ray.conf --head, it starts the ray master head
#   - run.sh start --config ray.conf, it starts ray slave nodes
#   - run.sh stop, stop ray

java -ea -cp "${CONF_DIR}:${LIB_DIR}/*" \
  -Dray.logging.file.name=ray-cli \
  -Dray.logging.path=${LOG_DIR} \
  org.ray.cli.RayCli "$@" \
  > ${LOG_DIR}/ray-cli.log 2>&1 &
