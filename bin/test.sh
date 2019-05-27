#!/usr/bin/env bash
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
$0 <h|d|w|m|t>  \"<date>\"\n
h\tHourly work, calculate data at previous hour, date format: yyyyMMddHH\n
d\tDaily work, calculate data on previous day, date format: yyyyMMdd\n
w\tWeekly work, calculate data in previous week, date format: yyyyMMdd\n
m\tMonthly work, calculate data in previous month, date format: yyyyMMdd\n
s\tImport data to HBase from MySQL\n
t\tTesting work, for developpers, date format depends on actual situation"

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

JAVA_OPTS="-Xmx2048m -Xmn256m "
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



# $0 service(master|worker)
function get_classname_by_service {
    if [ $# -ne 1 ];then
        return 1
    fi
    local service=$1
    if [ -z ${service} ];then
        return 1
    fi
    case ${service} in
      h)
        echo "com.dtc.analytics.works.Hourly"
        LOG_FILE="hourly.out.$2"
        PID_FILE="hourly.pid.$2"
        return 0
        ;;
      d)
        echo "com.dtc.analytics.works.Daily"
        LOG_FILE="daily.out.$2"
        PID_FILE="daily.pid.$2"
        retrun 0;
        ;;
      *)
        return 2
        ;;
esac
}


function main {
    # init
    service=
    cmd=
    force=
    waits=0

    [ $# -lt 2 ] && usage
    service=$1
    shift
    cmd=$1
    shift
    while getopts :w:f OPTION
    do
        case $OPTION in
            w)
                waits=$OPTARG
                ;;
            f)
                force="true"
                ;;
            \?)
                usage
                ;;
        esac
    done
    shift $(($OPTIND - 1))
    case ${cmd} in
        status)
            service_status ${service}
            ;;
        start)
            start_service_and_wait ${service} ${waits}
            ;;
        stop)
            if [ -z ${force} ];then
                stop_service_and_wait ${service} ${waits}
            else
                force_stop_service ${service}
            fi
            ;;
        restart)
            restart_service_and_wait ${service}
            ;;
        *)
            usage;
            ;;
    esac
}
main $@


