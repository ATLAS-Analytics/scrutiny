#!/bin/env bash

mkdir -p $ZEROACCESS_WITH_XAOD_OUT_DIR >/dev/null 2>&1
mkdir -p $ZEROACCESS_WITH_XAOD_CSV_OUT_DIR >/dev/null 2>&1

hadoop fs -cat scrutiny/zeroaccess_with_xaod/list-$DATE/* >$ZEROACCESS_WITH_XAOD_OUT_LIST

SCRIPT=$(readlink -f $0)
SCRIPTPATH=`dirname $SCRIPT`

cat $ZEROACCESS_WITH_XAOD_OUT_LIST | python $SCRIPTPATH/zeroaccess_stats.py top 20 >$ZEROACCESS_WITH_XAOD_CSV_OUT_DIR/top20_monthly.csv
cat $ZEROACCESS_WITH_XAOD_OUT_LIST | python $SCRIPTPATH/zeroaccess_stats.py full >$ZEROACCESS_WITH_XAOD_CSV_OUT_DIR/full_monthly.csv
