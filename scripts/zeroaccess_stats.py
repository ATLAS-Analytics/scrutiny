#!/bin/env python

import datetime
import fileinput
import re
import sys

"""
    Usage: cat list-2015-11-30 | ./stats_extended.py 

    Top/full stats choice in the end of the script.
"""


def round_timestamp(timestamp, granularity):
    if granularity not in ('day', 'week', 'month', 'year'):
        return

    date_ = datetime.date.fromtimestamp(float(timestamp))
    year, month, day = date_.year, date_.month, date_.day
    week = date_.strftime("%W")

    if granularity == "year":
        result = datetime.date(year, 1, 1)
    elif granularity == "month":
        result = datetime.date(year, month, 1)
    elif granularity == "day":
        result = datetime.date(year, month, day)
    elif granularity == "week":
        result = datetime.datetime.strptime("{0}-W{1}-1".format(year, week), "%Y-W%W-%w").date()

    return result


def compute_stats(lines):
    stats = {}

    for line in lines:
        line = line.strip(' \t\n\r')
        if line.startswith('#'):
            continue

        _, name, size, created_at = re.split(r'\t', line)[:4]
        project = name.split(".")[0]
        datatype = name.split('.')[4].split('_')[0]

        size = int(size or 0)
        creation_interval = int(datetime.date.fromtimestamp(int(created_at)).strftime("%s"))

        key = (project, datatype, creation_interval)
        if key not in stats:
            stats[key] = dict(count=0, size=0)
        stats[key]['count'] += 1
        stats[key]['size'] += int(size)
    return stats


def top_stats(stats, granularity, top=10):
    summary = {}
    for k, v in stats.iteritems():
        if k[1] == "log":
            ident = "log"
        else:
            ident = "{0}*{1}*".format(k[0], k[1])
        if ident is None:
            continue
        if ident not in summary:
            summary[ident] = 0
        summary[ident] += (v.get('size') or 0)
    sums = sorted(summary.items(), key=lambda x: x[1], reverse=True)

    topN = sums[:top]
    topN_list = [x[0] for x in topN]

    bins = {}
    for k, v in stats.iteritems():
        created_at = round_timestamp(k[2], granularity)

        if k[1] == "log":
            ident = "log"
        else:
            ident = "{0}*{1}*".format(k[0], k[1])
        if created_at not in bins:
            bins[created_at] = {}
        key = ident if ident in topN_list else "other"
        if key not in bins[created_at]:
            bins[created_at][key] = 0
        bins[created_at][key] += v["size"]

    result = []
    current_day = datetime.datetime.now().date()

    for interval, v in bins.iteritems():
        bin_age_days = (current_day-interval).days
        item = ["{0}".format(interval), bin_age_days,]

        for ident in topN_list + ['other',]:
            item.append(str(v.get(ident, 0)))
        result.append(item)
    
    result = sorted(result, reverse=True)
    result = [['Created at', 'Age (days)'] + topN_list+['other',],] + result
    result = [["# Series names in the next line"],] + result

    return result


def full_stats(stats, granularity):
    bins = {}
    for k, v in stats.iteritems():
        created_at = round_timestamp(k[2], granularity)
        ident = "{0}*{1}*".format(k[0], k[1])
        if created_at not in bins:
            bins[created_at] = {}
        if ident not in bins[created_at]:
            bins[created_at][ident] = 0
        bins[created_at][ident] += v["size"]

    result = []
    current_day = datetime.datetime.now().date()

    for interval, v in bins.iteritems():
        bin_age_days = (current_day-interval).days
        for ident in v:
            item = ["{0}".format(interval), bin_age_days,]
            item += [ident, str(v[ident]),]
            result.append(item)

    result = sorted(result, reverse=True)
    series = []
    for ident in [x[2] for x in result]:
        if ident not in series:
            series.append(ident)
    result = [['Created at', 'Age (days)', 'Project/datatype', 'Volume'],] + result
    result = [["# Column names in the next line"],] + result

    return result


def to_csv(items, lineterminator="\n"):
    result = ""
    for item in items:
        result += ",".join([str(x) for x in item]) + lineterminator
    return result


def main():
    args = sys.argv[1:]
    if not args or (args[0] not in ["full", "top"]):
        print("Usage: {0} STATS_TYPE [TOP_N]\nwhere STATS_TYPE could be 'full' or 'top'.\nFor the 'top', TOP_N limit could be specified (default is 20)".format(sys.argv[0]))
        sys.exit(1)
    stats = compute_stats(fileinput.input("-"))

    if args[0] == "top":
        topN = 20
        if len(args) > 1:
            topN = int(args[1])
        print to_csv(top_stats(stats, "month", top=topN))
    elif args[0] == "full":
        print to_csv(full_stats(stats, "month"))


if __name__ == "__main__":
    main()
