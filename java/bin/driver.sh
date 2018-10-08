#!/usr/bin/env bash

bin=`dirname ${0}`
bin=`cd ${bin}; pwd`
basedir=${bin}/..
LIB_DIR=${basedir}/lib
LOG_DIR=${basedir}/logs
CONF_DIR=${basedir}/conf
RUN_DIR=${basedir}/run
NATIVE_OPTS="-Djava.library.path=${basedir}/native"

# currently, the driver jar is stored under lib directory
# however, to support multiple drivers, we use jar to
# store user's driver file
JAR_DIR=${basedir}/jar

cd ${basedir}

if [ ! -d ${LOG_DIR} ]; then
  mkdir -p ${LOG_DIR}
fi
if [ ! -d ${RUN_DIR} ]; then
  mkdir -p ${RUN_DIR}
fi

# driver.sh ${driver_name} ${driver_jar} ${driver_class_name} ${driver_args}
if [ $# -lt 2 ]; then
    echo "Usage: driver.sh driver_name driver_jar driver_class_name driver_args"
    exit 1
fi

driver_name=$1
driver_jar=$2
driver_class_name=$3
shift 3
driver_args=$@

# put the user driver jar at the header of classpath
java -cp "${CONF_DIR}:${JAR_DIR}/${driver_jar}:${LIB_DIR}/*" \
  -Dray.logging.file.name=${driver_name} \
  -Dray.logging.path=${LOG_DIR} \
  ${NATIVE_OPTS} \
  ${driver_class_name} ${driver_args} \
  > ${LOG_DIR}/${driver_name}.log 2>&1 &

echo $! > ${RUN_DIR}/${driver_name}.pid
