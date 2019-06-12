#! /bin/bash
#
# Copyright 2017 Suning Inc.
# Created by Yan Jian on 2017/4/17.
#

# pre-defined envs
# export FM_HOME
# export FM_CONF_DIR


export HADOOP_USER_NAME="dtc"
export CONF_FILE="conf.properties"
export PLUGINS_DIRS=${FM_HOME}/plugins
export NATIVE_DIR=${FM_HOME}/native
export FM_USER=hadoop
export LOG_DIR=/home/${FM_USER}/logs

# defaults
#export EXEC="echo"
export EXEC="exec"
export FLINK_SUBMIT='/home/hadoop/software/flink/bin/flink'
export HOURLY_CLASS=com.dtc.analytics.works.Hourly
export FLINK_LOG_CLASS=com.dtc.analytic.scala.works.StreamingFlinkScala
export DAILY_CLASS=com.dtc.analytics.works.Daily
F
# common
FM_JAVA_LIBRARY_PATH=${NATIVE_DIR}
FM_CLASSPATH=${FM_CONF_DIR}:"${FM_HOME}/lib/common/*"

# load plugins.d directories
unset plugin_lib plugin_libext plugin_native;
for PLUGINS_DIR in $PLUGINS_DIRS; do
  if [[ -d ${PLUGINS_DIR} ]]; then
    for plugin in ${PLUGINS_DIR}/*; do
      if [[ -d "$plugin/lib" ]]; then
        plugin_lib="${plugin_lib}${plugin_lib+:}${plugin}/lib/*"
      fi
      if [[ -d "$plugin/libext" ]]; then
        plugin_libext="${plugin_libext}${plugin_libext+:}${plugin}/libext/*"
      fi
      if [[ -d "$plugin/native" ]]; then
        plugin_native="${plugin_native}${plugin_native+:}${plugin}/native"
      fi
    done
  fi
done
if [[ -n "${plugin_lib}" ]]
then
  FM_CLASSPATH="${FM_CLASSPATH}:${plugin_lib}"
fi

if [[ -n "${plugin_libext}" ]]
then
  FM_CLASSPATH="${FM_CLASSPATH}:${plugin_libext}"
fi

if [[ -n "${plugin_native}" ]]
then
  if [[ -n "${FM_JAVA_LIBRARY_PATH}" ]]
  then
    FM_JAVA_LIBRARY_PATH="${FM_JAVA_LIBRARY_PATH}:${plugin_native}"
  else
    FM_JAVA_LIBRARY_PATH="${plugin_native}"
  fi
fi

export FM_JAVA_LIBRARY_PATH
export FM_CLASSPATH

# master
MASTER_OPTS="
-Xmx2048m
-Xms2048m
-XX:+AlwaysPreTouch
-XX:+UseG1GC
-XX:InitiatingHeapOccupancyPercent=80
-XX:+ParallelRefProcEnabled
-XX:ParallelGCThreads=8
-XX:ConcGCThreads=2
-XX:MaxGCPauseMillis=500
-XX:MaxDirectMemorySize=512m
-XX:+HeapDumpOnOutOfMemoryError
-XX:HeapDumpPath=/home/${FM_USER}/logs/master-dump.hprof
-verbose:gc
-XX:+PrintGCDetails
-XX:+PrintGCDateStamps
-Xloggc:/home/${FM_USER}/logs/master-gc.log
"


WORKER_OPTS="
-Xmx4096m
-Xms4096m
-XX:+AlwaysPreTouch
-XX:+UseG1GC
-XX:InitiatingHeapOccupancyPercent=80
-XX:+ParallelRefProcEnabled
-XX:ParallelGCThreads=8
-XX:ConcGCThreads=2
-XX:MaxGCPauseMillis=500
-XX:MaxDirectMemorySize=512m
-XX:+HeapDumpOnOutOfMemoryError
-XX:HeapDumpPath=/home/${FM_USER}/logs/worker-dump.hprof
-verbose:gc
-XX:+PrintGCDetails
-XX:+PrintGCDateStamps
-Xloggc:/home/${FM_USER}/logs/worker-gc.log
-Dcom.sun.management.jmxremote.ssl=false
-Dcom.sun.management.jmxremote.authenticate=false
-Dcom.sun.management.jmxremote.port=7109
-Dfm.log.name=worker
"



# $0 -
function start_h {
    if ! which java >/dev/null 2>&1 ; then
        return 2;
    fi
    ${EXEC} java -XX:OnOutOfMemoryError="kill -9 %p" \
            -Djava.library.path=${FM_JAVA_LIBRARY_PATH} ${MASTER_OPTS} -cp ${FM_CLASSPATH} ${HOURLY_CLASS} ${FM_CONF_DIR}
    return 0
}

#function start_flink-log {
#    echo "enter to submit flink job-------------------"
#    if ! which java >/dev/null 2>&1 ; then
#        return 2;
#    fi
#    if ! which scala >/dev/null 2>&1 ; then
#        return 2;
#    fi
#    echo "starting to submit flink job-------------------"
#    ${FLINK_SUBMIT} -c ${FLINK_LOG_CLASS} ${FM_HOME}/lib/common/dtc-flink-0.1.0.jar
#    local test=$?
#    echo ${test}
#    if [ ${test} -ne 0 ];then
#        echo "submit is failed-------------------!"
#    else
#        echo "submit is success-------------------!!"
#    fi
#}

# $0 -
function start_d {
    if ! which java >/dev/null 2>&1 ; then
        return 2;
    fi
    ${EXEC} java -XX:OnOutOfMemoryError="kill -9 %p" \
            -Djava.library.path=${FM_JAVA_LIBRARY_PATH} ${WORKER_OPTS} -cp ${FM_CLASSPATH} ${DAILY_CLASS} ${FM_CONF_DIR}
    return 0
}
