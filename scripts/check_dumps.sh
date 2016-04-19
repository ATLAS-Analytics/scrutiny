#!/usr/bin/env bash

export HDFS_RUCIO_DIR="/user/rucio01"
export HDFS_RUCIO_DUMPS_DIR="${HDFS_RUCIO_DIR}/dumps"

dumps=(
    "contents"
    "dids"
    "dslocks"
    "rses"
)

today=$(date -I -d "today")

for d in "${dumps[@]}"; do
    while true; do
        hour=$(date +%k)
        if [ $hour -gt 9 ]; then
            echo "too late"
            exit 1
        fi

        echo "check $HDFS_RUCIO_DUMPS_DIR/${today}/${d}/_SUCCESS"
        hadoop fs -ls "$HDFS_RUCIO_DUMPS_DIR/${today}/${d}/_SUCCESS" > /dev/null 2>&1
        if [ $? -ne 0 ]; then
            echo "could not find ${d}, let's sleep for 30 minutes and see again"
            sleep 10
        else
            echo "found ${d}, continue with the next dump"
            break
        fi
    done
done

echo "found everything. The processing may commence."
