#!/usr/bin/env bash

# DATE variable should be set before calling the script

# Send notifications to:
export MAIL_TO=atlstats@cern.ch

export SCRIPTS_PATH=$BASE_DIR/scripts

export PIG_DIR=$BASE_DIR/scripts/pig

export LIB_DIR=$BASE_DIR/lib

# Directories on HDFS
export HDFS_SCRUTINY_DIR="/user/atlstats/scrutiny"
export HDFS_ALL_REPORTS_DIR="${HDFS_SCRUTINY_DIR}/reports"
export HDFS_TRACES_DIR="${HDFS_SCRUTINY_DIR}/traces-preprocess"
export HDFS_ZEROACCESS_DIR="${HDFS_SCRUTINY_DIR}/zeroaccess"
export HDFS_ZEROACCESS_WITH_XAOD_DIR="${HDFS_SCRUTINY_DIR}/zeroaccess_with_xaod"
export HDFS_REPORT_DIR=$HDFS_ALL_REPORTS_DIR/$DATE

export HDFS_XAOD_SCRUTINY_DIR="/user/atlstats/xaod_scrutiny"
export HDFS_XAOD_TRACES_DIR="${HDFS_XAOD_SCRUTINY_DIR}/traces-files"

export HDFS_RUCIO_DIR="/user/rucio01"
export HDFS_RUCIO_DUMPS_DIR="${HDFS_RUCIO_DIR}/dumps"

export POPULARITY_CSV_OUT_DIR=$HOME/www/scrutiny/$DATE/csv

export ZEROACCESS_OUT_DIR=$HOME/www/zeroaccess/$DATE
export ZEROACCESS_OUT_LIST=$ZEROACCESS_OUT_DIR/list-$DATE
export ZEROACCESS_CSV_OUT_DIR=$ZEROACCESS_OUT_DIR/csv

export ZEROACCESS_WITH_XAOD_OUT_DIR=$HOME/www/zeroaccess_with_xaod/$DATE
export ZEROACCESS_WITH_XAOD_OUT_LIST=$ZEROACCESS_WITH_XAOD_OUT_DIR/list-$DATE
export ZEROACCESS_WITH_XAOD_CSV_OUT_DIR=$ZEROACCESS_WITH_XAOD_OUT_DIR/csv

export XAOD_OUT_DIR=$HOME/www/xaod_scrutiny/$DATE/csv

export LOG_DIR=$HOME/private/scrutiny/logs
