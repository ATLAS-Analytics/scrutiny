#!/usr/bin/env bash

YEAR=`echo $DATE | cut -f 1 -d "-"`

cd $LOGS # Some logs are being written to the current directory

# Zero-access statistics
TIME_UTC=$(($(date -d $DATE +%s) + 86400)) # Taking the whole day
LOG=`pig -f $PIG_DIR/zeroaccess_with_xaod.pig -p DATE=$DATE -p TIME_UTC=$TIME_UTC -p OUT_FILE=${HDFS_ZEROACCESS_WITH_XAOD_DIR}/list-${DATE} -l "$LOGS" 2>&1`

CODE=$?

if [ $CODE -eq 0 ]; then
    echo "Done" | mail -s "Scrutiny: finished for zeroaccess with xaod" $MAIL_TO
else
    echo "$LOG" | mail -s "Scrutiny: falied for zeroaccess with xaod" $MAIL_TO
fi

echo $LOG

exit $CODE
