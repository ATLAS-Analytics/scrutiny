#!/bin/env bash

mkdir -p $XAOD_OUT_DIR >/dev/null 2>&1

TMP_OUT_FILE=$XAOD_OUT_DIR/tmp_aod_vs_daod.csv

hadoop fs -cat $HDFS_XAOD_SCRUTINY_DIR/xaod_aod_vs_daod_$START_MONTH-$END_MONTH/part* > $TMP_OUT_FILE

SCRIPT=$(readlink -f $0)
SCRIPTPATH=`dirname $SCRIPT`

python $SCRIPTPATH/xaod_stats.py $TMP_OUT_FILE  events > $XAOD_OUT_DIR/events.csv
python $SCRIPTPATH/xaod_stats.py $TMP_OUT_FILE bytes > $XAOD_OUT_DIR/bytes.csv

rm $TMP_OUT_FILE
