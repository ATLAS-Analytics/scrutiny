#!/bin/env bash

mkdir -p $XAOD_OUT_DIR >/dev/null 2>&1

hadoop fs -cat '$HDFS_XAOD_SCRUTINY_DIR/xaod_aod_vs_daod_$START_MONTH-$END_MONTH/part*' > $XAOD_STATS

SCRIPT=$(readlink -f $0)
SCRIPTPATH=`dirname $SCRIPT`

cat $XAOD_STATS | python $SCRIPTPATH/xaod_stats.py events > $XAOD_OUT_DIR/events.csv
cat $XAOD_STATS | python $SCRIPTPATH/xaod_stats.py bytes > $XAOD_OUT_DIR/bytes.csv
