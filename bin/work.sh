#!/bin/bash -l
# Don't edit this file unless you know exactly what you're doing.

run () {
  if [ -f $RUN_PATH/$PID_FILE ]; then
    echo "$RUN_PATH/$PID_FILE already exists."
    echo "Now exiting ..."
    exit 1
  fi
  $@ > $LOG_PATH/$LOG_FILE 2>&1 &
  PID=$!
  echo $PID > "$RUN_PATH/$PID_FILE"
  wait $PID
  rm -f $RUN_PATH/$PID_FILE
}

usage="Usage:\n
$0 <h|d>  \"<date>\"\n
h\tHourly work, calculate data at previous hour, date format: yyyyMMddHH\n
d\tDaily work, calculate data on previous day, date format: yyyyMMdd\n"

if [ $# -lt 2 ]; then
  echo -e $usage
  exit 1
fi

BIN_DIR=$(cd $(dirname $0); pwd)
. $BIN_DIR/env.sh

CONF_PATH=$RAZOR_MR_HOME/conf:$HADOOP_CONF:$HBASE_CONF
CLASSPATH="$CONF_PATH:$RAZOR_MR_HOME/lib/*:$HADOOP_CLASSPATH:$HBASE_CLASSPATH:$CLASSPATH"
LOG_PATH=$RAZOR_MR_HOME/logs
RUN_PATH=$RAZOR_MR_HOME/run

JAVA_OPTS="
-Xmx2048m
-Xms1024m
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
"
JAVA=""
if [ "$JAVA_HOME" != "" ] ; then
  JAVA=$JAVA_HOME/bin/java
else
  echo "Environment variable \$JAVA_HOME is not set."
  exit 1
fi

if [ ! -d $LOG_PATH ];then
  mkdir -p $LOG_PATH
fi

if [ ! -d $RUN_PATH ];then
  mkdir -p $RUN_PATH
fi 

case $1 in
  h)
    CLASS="com.dtc.analytics.works.Hourly"
    LOG_FILE="hourly.out.$2"
    PID_FILE="hourly.pid.$2"
    ;;
  d)
    CLASS="com.dtc.analytics.works.Daily"
    LOG_FILE="daily.out.$2"
    PID_FILE="daily.pid.$2"
    ;;
  *)
esac

CMD="$JAVA -Dsun.jnu.encoding=UTF-8 -Dfile.encoding=UTF-8 -cp $CLASSPATH $JAVA_OPTS $CLASS ${@:2}"
echo "$CMD"
run "$CMD" &

