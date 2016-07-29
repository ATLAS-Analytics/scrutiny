#!/bin/bash

cd $LOGS

MONTH=$START_MONTH

MONTHS=$MONTH
MONTH=$(/bin/date --date "$MONTH-01 1 month" +%Y-%m)

failed=0
while [ "$MONTH" != "$END_MONTH" ]
do
    MONTHS="$MONTHS,$MONTH"
    MONTH=$(/bin/date --date "$MONTH-01 1 month" +%Y-%m)
done

OUTPUT_MONTHS="$START_MONTH-$END_MONTH"

DIDS_DATE=$(/bin/date --date "$END_MONTH-01 -1 day" +%Y-%m-%d)

LOG=`pig -p LIB_DIR=$LIB_DIR -p DIDS_DATE=$DIDS_DATE -p HDFS_XAOD_TRACES_DIR=$HDFS_XAOD_TRACES_DIR -p MONTHS=$MONTHS -p HDFS_XAOD_SCRUTINY_DIR=$HDFS_XAOD_SCRUTINY_DIR -p HDFS_RUCIO_DIR=$HDFS_RUCIO_DIR -p OUTPUT_MONTHS=$OUTPUT_MONTHS -f $PIG_DIR/xaod_aod_vs_daod.pig`

CODE=$?

if [ $? -eq 0 ]; then
    echo "Done" | mail -s "XAOD Scrutiny: finished processing" $MAIL_TO
else
    echo "$LOG" | mail -s "XAOD Scrutiny: failed processing" $ $MAIL_TO
fi

exit $CODE
