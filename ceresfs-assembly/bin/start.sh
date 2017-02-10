#!/bin/bash
cd `dirname $0`
DEPLOY_DIR=`pwd`
LIB_DIR=${DEPLOY_DIR}/lib
STDOUT_FILE=${DEPLOY_DIR}/ceresfs.out

PID=`ps -f | grep java | grep "$DEPLOY_DIR" | awk '{print $2}'`

ps -f | grep java | grep "$DEPLOY_DIR" | awk '{print $2}'
if [[ -n ${PID} ]]; then
    echo "ERROR: The application is already running!"
    exit 1
fi

JAVA_OPTS=" -server -Xmx4G -Djava.awt.headless=true -Djava.net.preferIPv4Stack=true "

echo 'Starting CeresFS...'
exec -a CeresFS nohup java -jar ${DEPLOY_DIR}/lib/ceresfs-server-1.0-SNAPSHOT.jar > ${STDOUT_FILE} 2>&1 & 
echo 'OK!'
echo "STDOUT: ${STDOUT_FILE}"
