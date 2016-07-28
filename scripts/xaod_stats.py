#!/usr/bin/env python

from sys import argv
from datetime import datetime

data = {}

with open(argv[1], 'r') as f:
    for line in f:
        day, type, acc_type, events, bytes, cnt = line.strip().split('\t')
        if events != '':
            events = int(events)
        else:
            events = 0
        bytes = int(bytes)
        cnt = int(cnt)

        if day not in data:
            data[day] = {}

        if acc_type not in data[day]:
            data[day][acc_type] = {}

        data[day][acc_type][type] = (events, bytes, cnt)

aggs = {}
        
for day, acc_types in data.items():
    week = datetime.strptime(day, '%Y-%m-%d').isocalendar()[1]
    if week not in aggs:
        aggs[week] = {}

    for acc_type, types in acc_types.items():
        if acc_type not in aggs[week]:
            aggs[week][acc_type] = {}

        for type, cnts in types.items():
            if type not in aggs[week][acc_type]:
                aggs[week][acc_type][type] = [0, 0, 0]

            aggs[week][acc_type][type][0] += cnts[0]
            aggs[week][acc_type][type][1] += cnts[1]
            aggs[week][acc_type][type][2] += cnts[2]


if argv[2] == 'events':
    i = 0
elif argv[2] == 'bytes':
    i = 1
else:
    i = 2
    
for week, acc_types in sorted(aggs.items()):
    outline = '\t'.join((str(week), str(acc_types['grid']['AOD'][i]), str(acc_types['local']['AOD'][i]), str(acc_types['grid']['DAOD'][i]), str(acc_types['local']['DAOD'][i])))
    print(outline)
