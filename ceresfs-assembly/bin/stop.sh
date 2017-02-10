#!/bin/bash
cd `dirname $0`
DEPLOY_DIR=`pwd`
LIB_DIR=${DEPLOY_DIR}/lib
STDOUT_FILE=${DEPLOY_DIR}/ceresfs.out

PID=`ps -f | grep java | grep "$DEPLOY_DIR" | awk '{print $2}'`

if [[ -z ${PID//} ]]; then
    echo "ERROR: The application does not started!"
    exit 1
fi

echo -e "Stopping the application ...\c"
kill $PID
echo "OK"
echo "PID: $PID"
