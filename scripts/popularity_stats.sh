#!/bin/env bash

mkdir -p $POPULARITY_CSV_OUT_DIR

read -d '' STATS_SCRIPT <<EOT
  local %s;
  for (-1..15) {
    \$s{\$_} = 0;
  }
  
  for (<>) {
    m/(-?\\\w+),(\\\w+)/;
    if ( \$1 + 0 < 15) {
      \$s{\$1+0} += int(\$2);
    } else {
      \$s{15} += int(\$2);
    }
  }

  for (keys %s) {
    print "\$_,\$s{\$_}\\\n"; 
  }
EOT

for months in 3 6 9 12 "infinity";
do
    if [ "$months" != "infinity" ];
    then
        months=`printf "%0*dmonths" 2 $months`
    fi

    hadoop fs -cat $HDFS_REPORT_DIR/${months}.csv/* | perl -e "$STATS_SCRIPT" | sort -g >$POPULARITY_CSV_OUT_DIR/${months}.csv
done
