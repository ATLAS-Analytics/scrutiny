#!/usr/bin/env bash

# DATE variable should be set before calling the script

# Send notifications to:
export MAIL_TO=atlstats@cern.ch

export SCRIPTS_PATH=$BASE_DIR/scripts

export PIG_DIR=$BASE_DIR/scripts/pig

# Directories on HDFS
export HDFS_SCRUTINY_DIR="/user/atlstats/scrutiny"
export HDFS_ALL_REPORTS_DIR="${HDFS_SCRUTINY_DIR}/reports"
export HDFS_TRACES_DIR="${HDFS_SCRUTINY_DIR}/traces-preprocess"
export HDFS_ZEROACCESS_DIR="${HDFS_SCRUTINY_DIR}/zeroaccess"
export HDFS_REPORT_DIR=$HDFS_ALL_REPORTS_DIR/$DATE

export POPULARITY_CSV_OUT_DIR=$HOME/www/scrutiny/$DATE/csv

export ZEROACCESS_OUT_DIR=$HOME/www/zeroaccess/$DATE
export ZEROACCESS_OUT_LIST=$ZEROACCESS_OUT_DIR/list-$DATE
export ZEROACCESS_CSV_OUT_DIR=$ZEROACCESS_OUT_DIR/csv

export LOG_DIR=$HOME/private/scrutiny/logs
