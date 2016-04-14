#!/bin/env bash

THIS=$(readlink -f $0)
export BASE_DIR=`dirname $THIS`
export DATE=`date +%Y-%m-%d`

. $BASE_DIR/config/config.sh

export LOGS="$LOG_DIR/$(date +%F_%H%M)"
mkdir -p $LOGS

$SCRIPTS_PATH/produce.sh >$LOGS/produce.log 2>&1

if [ $? -eq 0 ]; then
    exit $?
fi

$SCRIPTS_PATH/popularity_stats.sh >$LOGS/popularity_stats.log 2>&1
$SCRIPTS_PATH/zeroaccess_stats.sh >$LOGS/zeroaccess_stats.log 2>&1
