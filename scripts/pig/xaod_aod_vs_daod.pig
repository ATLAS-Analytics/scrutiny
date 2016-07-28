set job.name atlas-ddm-scrutiny-xaod-aod-vs-daod;

register '$LIB_DIR/resolve_udfs.py' USING jython AS udfs;
REGISTER /usr/lib/pig/lib/avro.jar;
register /usr/lib/avro/avro-mapred.jar;

files = LOAD '$HDFS_XAOD_TRACES_DIR/{$MONTHS}' USING PigStorage() AS (
 timeentry: long,
 uuid: chararray,
 id: long,
 path: chararray,
 rate: int
);

get_filename = FOREACH files GENERATE udfs.toDay(timeentry) as day, uuid, id, udfs.getFilename(path) as filename, udfs.getTraceType(id) as traceType;

get_type = FOREACH get_filename GENERATE day, uuid, id, filename, traceType, udfs.getType(filename) as type, udfs.getDAODType(filename) as daodType;

dids = LOAD '$HDFS_RUCIO_DIR/dumps/2016-06-30/dids' USING AvroStorage();

get_events = FOREACH dids GENERATE SCOPE as scope, NAME as name, (long)BYTES as bytes, (long)EVENTS as events, DID_TYPE as did_type;

filter_files = FILTER get_events BY did_type == 'F';

join_files_dids = JOIN get_type BY filename, filter_files BY name;

get_join_fields = FOREACH join_files_dids GENERATE get_type::day as day, get_type::type as type, get_type::uuid as uuid, get_type::filename as filename, filter_files::events as events, filter_files::bytes as bytes, get_type::traceType as traceType;

group_traces = GROUP get_join_fields BY (day, uuid, filename);

get_traces = FOREACH group_traces GENERATE group.day as day, MAX(get_join_fields.type) as type, MIN(get_join_fields.events) as events, MIN(get_join_fields.bytes) as bytes, MIN(get_join_fields.traceType) as traceType;

group_types = GROUP get_traces BY (day, type, traceType);

sum_up = FOREACH group_types GENERATE group.day as day, group.type as type, group.traceType as traceType, SUM(get_traces.events) as events, SUM(get_traces.bytes) as bytes, COUNT(get_traces) as c;

order_day = ORDER sum_up BY day ASC, type ASC, traceType ASC;

STORE order_day INTO '$HDFS_XAOD_SCRUTINY_DIR/xaod_aod_vs_daod_$OUTPUT_MONTHS' USING PigStorage('\t');
