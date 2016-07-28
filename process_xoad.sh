#!/bin/env bash

THIS=$(readlink -f $0)
export BASE_DIR=`dirname $THIS`
export DATE=`date +%Y-%m-%d`

. $BASE_DIR/config/config.sh

export LOGS="$LOG_DIR/xoad_$(date +%F_%H%M)"
mkdir -p $LOGS

export START_MONTH=$1
export END_MONTH=$2

echo $START_MONTH
echo $END_MONTH

$SCRIPTS_PATH/xaod_to_files.sh

if [ $? -eq 1 ]; then
    exit 1
fi

$SCRIPTS_PATH/xaod_aod_vs_daod.sh

if [ $? -eq 1 ]; then
    exit 1
fi

$SCRIPTS_PATH/xaod_stats.sh
