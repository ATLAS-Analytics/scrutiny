#!/usr/bin/env bash

YEAR=`echo $DATE | cut -f 1 -d "-"`

cd $LOGS # Some logs are being written to the current directory

# Preprocess traces
hadoop fs -mkdir -p "${HDFS_TRACES_DIR}" >/dev/null 2>&1
hadoop fs -rm -r "${HDFS_TRACES_DIR}/${YEAR}_fresh" >/dev/null 2>&1

LOG=`pig -F -f $PIG_DIR/preprocess_traces.pig -p YEAR=$YEAR -p DATE=$DATE -p OUT_FILE="${HDFS_TRACES_DIR}/${YEAR}_fresh" -l "$LOGS" 2>&1`
CODE=$?

if [ $CODE -eq 0 ]; then
    echo "Done" | mail -s "Scrutiny: traces preprocessing finished" $MAIL_TO
    echo $LOG
else
    echo "$LOG" | mail -s "Scrutiny: traces preprocessing failed" $MAIL_TO
    echo $LOG
    exit $CODE
fi

# Replace preprocesed traces
hadoop fs -rm -r "${HDFS_TRACES_DIR}/${YEAR}" >/dev/null 2>&1
hadoop fs -rm -r "${HDFS_TRACES_DIR}/${YEAR}" >/dev/null 2>&1
hadoop fs -mv "${HDFS_TRACES_DIR}/${YEAR}_fresh" "${HDFS_TRACES_DIR}/${YEAR}"

# Prepare report direcroties
hadoop fs -rm -r "$HDFS_REPORT_DIR" >/dev/null 2>&1
hadoop fs -mkdir -p "$HDFS_REPORT_DIR" >/dev/null 2>&1

# Per-interval statistics
for months in 3 6 9 12 "infinity";
do
    if [ "$months" != "infinity" ];
    then
        TRACES_TIME=$(($(date +%s)-$months*30*86400))
        months=`printf "%0*dmonths" 2 $months`
    else
        TRACES_TIME=0
    fi

    LOG=`pig -f $PIG_DIR/report.pig -p DATE=$DATE -p TRACES_TIME=$TRACES_TIME -p DATASET_AGE=$TRACES_TIME -p OUT_FILE=${HDFS_REPORT_DIR}/${months}.csv -l "$LOGS" 2>&1`

    if [ $? -eq 0 ]; then
        echo "Done" | mail -s "Scrutiny: finished for $months" $MAIL_TO
    else
        echo "$LOG" | mail -s "Scrutiny: failed for $months" $MAIL_TO
    fi
    
    echo $LOG
done

# Zero-access statistics
TIME_UTC=$(($(date -d $DATE +%s) + 86400)) # Taking the whole day
LOG=`pig -f $PIG_DIR/zeroaccess.pig -p DATE=$DATE -p TIME_UTC=$TIME_UTC -p OUT_FILE=${HDFS_ZEROACCESS_DIR}/list-${DATE} -l "$LOGS" 2>&1`

CODE=$?

if [ $CODE -eq 0 ]; then
    echo "Done" | mail -s "Scrutiny: finished for zeroaccess" $MAIL_TO
else
    echo "$LOG" | mail -s "Scrutiny: falied for zeroaccess" $MAIL_TO
fi

echo $LOG

exit $CODE
