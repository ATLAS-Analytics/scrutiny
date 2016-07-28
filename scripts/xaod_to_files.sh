#!/bin/bash

cd $LOGS

MONTH=$START_MONTH

FAILED=0
while [ "$MONTH" != "$END_MONTH" ]
do
    echo "checking $HDFS_XAOD_TRACES_DIR/$M/_SUCCESS"
    hadoop fs -ls "$HDFS_XAOD_TRACES_DIR/$M/_SUCCESS" > /dev/null 2>&1
    if [ $? -eq 0 ]; then
        echo "already there, no need to process again"
        continue
    fi
    
    LOG='pig -p LIB_DIR=$LIB_DIR -p HDFS_RUCIO_DIR=$HDFS_RUCIO_DIR -p HDFS_XAOD_TRACES_DIR=$HDFS_XAOD_TRACES_DIR -p MONTH=$MONTH -f $PIG_DIR/xaod_to_files.pig'

    if [ $? -eq 0 ]; then
        echo "Done" | mail -s "XAOD Scrutiny: finished resolving to files for $MONTH" $MAIL_TO
    else
        echo "$LOG" | mail -s "XAOD Scrutiny: failed resolving to files for $MONTH" $ $MAIL_TO
        FAILED=1
    fi

    MONTH=$(/bin/date --date "$MONTH-01 1 month" +%Y-%m)
done

exit $FAILED
