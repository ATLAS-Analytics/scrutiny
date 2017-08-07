#!/bin/env bash

THIS=$(readlink -f $0)
export BASE_DIR=`dirname $THIS`
#export DATE=`date +%Y-%m-%d`
export DATE=2017-01-16

. $BASE_DIR/config/config.sh

export LOGS="$LOG_DIR/$(date +%F_%H%M)"
mkdir -p $LOGS

$SCRIPTS_PATH/check_dumps.sh > $LOGS/check_dumps.log 2>&1

if [ $? -eq 1 ]; then
    mail -s "Scrutiny: dumps check failed, could not find all necessary dumps" $MAIL_TO
    exit $?
fi

$SCRIPTS_PATH/produce_with_xaod.sh >$LOGS/produce_with_xaod.log 2>&1

if [ $? -ne 0 ]; then
    exit $?
fi

$SCRIPTS_PATH/zeroaccess_stats_with_xaod.sh >$LOGS/zeroaccess_stats_with_xaod.log 2>&1
