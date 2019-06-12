#!/usr/bin/env bash
# Don't edit this file unless you know exactly what you're doing.

export PATH=/etc:/bin:/sbin:/usr/bin:/usr/sbin:/usr/local/bin:/usr/local/sbin:/home/dtc/software/java/bin
if [ -z "${LOG_DIR}" ];then
    LOG_DIR=/home/dtc/logs
fi
export LOG_DIR

if [ -z "${FM_HOME}" ];then
    FM_HOME=$(cd `dirname $0`;cd ..; pwd)
fi
export FM_HOME

if [ -z "${FM_CONF_DIR}" ] || [ ! -d "${FM_CONF_DIR}" ];then
    FM_CONF_DIR=${FM_HOME}/conf
fi
export FM_CONF_DIR

if [ -f ${FM_CONF_DIR}/fm-env.sh ];then
    source ${FM_CONF_DIR}/fm-env.sh
fi

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


BIN_DIR=$(cd $(dirname $0); pwd)
. $BIN_DIR/env.sh

LOG_PATH=$ROOT/logs
RUN_PATH=$ROOT/run

JAVA_OPTS="-Xmx2048m -Xmn256m "
FLINK_SUBMIT='/usr/bin/flink run'
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

CLASS="com.dtc.analytic.scala.works.StreamingFlinkScala"
LOG_FILE="cd-streaming.out"
PID_FILE="cd-streaming.pid"

CMD="$FLINK_SUBMIT -c $CLASS $FM_HOME/lib/common/dtc-flink-0.1.0.jar
  --properties-file $ROOT/conf/razor-spark.conf \
  --class $CLASS --master ${MASTER:-yarn-cluster} \
  $ROOT/razor-spark-0.1-SNAPSHOT.jar ${@:2}"
echo -e "$CMD"
run "$CMD" &



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

# main $@ | tee -a ${LOG_DIR}/${service}.out 3>&1 1>&2 2>&3 | tee -a ${LOG_DIR}/${service}.err
main $@

