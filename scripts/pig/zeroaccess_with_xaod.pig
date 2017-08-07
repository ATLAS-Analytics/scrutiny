-- 

SET job.name atlas-ddm-scrutiny-zeroaccess;

REGISTER '/usr/lib/pig/piggybank.jar';
REGISTER '/usr/lib/avro/avro.jar';
REGISTER '/usr/lib/avro/avro-mapred.jar';
REGISTER '/usr/lib/pig/lib/json-simple-1.1.jar';

REGISTER '/afs/cern.ch/user/a/atlstats/public/scrutiny/lib/rucioudfs.jar';

DEFINE ToISO org.apache.pig.piggybank.evaluation.datetime.convert.CustomFormatToISO();
DEFINE ToUnix org.apache.pig.piggybank.evaluation.datetime.convert.ISOToUnix();


-- Datasets' replicas
dslocks = LOAD '/user/rucio01/dumps/$DATE/dslocks' USING AvroStorage();

-- Datasets' information
dids = LOAD '/user/rucio01/dumps/$DATE/dids' USING AvroStorage();

-- Rucio storage elements
rses = LOAD '/user/rucio01/dumps/$DATE/rses' USING AvroStorage();

-- Dataset replicas (with incomplete)
collection_replicas = LOAD '/user/rucio01/dumps/$DATE/collection_replicas' USING AvroStorage();

-- Load preprocessed traces up to Aug 2016
traces1 = LOAD '/user/rucio01/tmp/processed_traces_30_07_2015/*' USING PigStorage('\t') AS (
  scope: chararray,
  name: chararray,
  uuid: chararray,
  fileops: long,
  timeentryunix: double
);

-- Load newer traces
traces2 = LOAD 'scrutiny/traces-preprocess/*/*' USING PigStorage('\t') AS (
  scope: chararray,
  name: chararray,
  uuid: chararray,
  fileops: long,
  timeentryunix: double
);

xaod_traces = LOAD '/user/rucio01/tmp/xaod_traces_datasets_201*' USING PigStorage('\t') AS (
 timeentry: long,
 uuid: chararray,
 path: chararray,
 filename: chararray,
 scope: chararray,
 name: chararray,
 rate: int
);


reduce_fields_xaod = FOREACH xaod_traces GENERATE scope, name, uuid, timeentry;

group_xaod = GROUP reduce_fields_xaod BY (scope, name);

xaod_get_ops = FOREACH group_xaod GENERATE group.scope as scope, group.name as name, COUNT(reduce_fields_xaod) as op_events;

traces = UNION traces1, traces2;

-- Drop unused columns

reduce_fields_dids = FOREACH dids GENERATE SCOPE as scope, NAME as name, TRANSIENT as transient, CREATED_AT/1000.0 as created_at;

filter_data_mc = FILTER reduce_fields_dids BY (transient IS NULL) AND (NOT (name MATCHES '.*_sub.*')) AND ((scope MATCHES 'data.*') OR (scope MATCHES 'mc.*') OR (scope MATCHES 'valid.*'));

reduce_fields_dids_2 = FOREACH filter_data_mc GENERATE scope, name, created_at;

-- RSEs with IDs
reduce_fields_rses = FOREACH rses GENERATE ID AS id, RSE AS rse;

reduce_fields_dslocks = FOREACH dslocks GENERATE SCOPE as scope, NAME as name, RSE_ID as rse_id, BYTES as bytes;

join_dslocks_rses = JOIN reduce_fields_dslocks BY rse_id, reduce_fields_rses BY id USING 'REPLICATED';

get_dslocks = FOREACH join_dslocks_rses GENERATE reduce_fields_dslocks::scope as scope, reduce_fields_dslocks::name as name, reduce_fields_rses::rse as rse, (long)reduce_fields_dslocks::bytes as bytes, reduce_fields_dslocks::rse_id as rse_id;

get_datadisk_locks = FILTER get_dslocks BY (rse MATCHES '.*DATADISK');

reduce_fields_coll_reps = FOREACH collection_replicas GENERATE SCOPE as scope, NAME as name, RSE_ID as rse_id, (long)AVAILABLE_BYTES as bytes;

join_dslocks_coll_reps = JOIN get_datadisk_locks BY (scope, name, rse_id), reduce_fields_coll_reps BY (scope, name, rse_id);

get_replicas = FOREACH join_dslocks_coll_reps GENERATE get_datadisk_locks::scope as scope, get_datadisk_locks::name as name, get_datadisk_locks::rse as rse, reduce_fields_coll_reps::bytes as bytes;

--get_replicas = FOREACH join_dslocks_rses GENERATE reduce_fields_dslocks::scope as scope, reduce_fields_dslocks::name as name, reduce_fields_rses::rse as rse, (long)reduce_fields_dslocks::bytes as bytes;

join_dids_replicas = JOIN reduce_fields_dids_2 BY (scope, name), get_replicas BY (scope, name);

get_primary_datadisk = FOREACH join_dids_replicas GENERATE get_replicas::scope as scope, get_replicas::name as name, get_replicas::bytes as bytes, get_replicas::rse as rse, reduce_fields_dids_2::created_at AS created_at;

group_primary_datadisk = GROUP get_primary_datadisk BY (scope, name, rse);

get_size_primary_datadisk = FOREACH group_primary_datadisk GENERATE group.scope as scope, group.name as name, group.rse as rse, MAX(get_primary_datadisk.bytes) as bytes, MIN(get_primary_datadisk.created_at) AS created_at;

filter_traces_time = FILTER traces BY timeentryunix IS NOT NULL;

group_traces = GROUP filter_traces_time BY (scope, name);
count_events = FOREACH group_traces GENERATE group.scope as scope, group.name as name, COUNT(filter_traces_time.uuid) as op_events;

all_traces = UNION count_events, xaod_get_ops;

group_all_traces = GROUP all_traces BY (scope, name);

get_events = FOREACH group_all_traces GENERATE group.scope as scope, group.name as name, SUM(all_traces.op_events) as op_events;

join_traces_primaries = JOIN get_size_primary_datadisk BY (scope, name) LEFT OUTER, get_events BY (scope, name);

get_event_count = FOREACH join_traces_primaries GENERATE get_size_primary_datadisk::scope, get_size_primary_datadisk::created_at AS created_at, get_size_primary_datadisk::name as name, get_size_primary_datadisk::rse as rse, get_size_primary_datadisk::bytes AS bytes, ((get_events::op_events IS NULL) ? 0 : get_events::op_events) AS op_events;

group_scope_name = GROUP get_event_count BY (scope, name);

get_dataset_volume = FOREACH group_scope_name GENERATE group.scope as scope, group.name as name, MIN(get_event_count.created_at) AS created_at, MAX(get_event_count.op_events) AS op_events, SUM(get_event_count.bytes) AS bytes;

filter_zero_ = FILTER get_dataset_volume BY op_events == 0;
filter_zero = FOREACH filter_zero_ GENERATE scope, name, bytes, (int)created_at, (int)(($TIME_UTC - created_at)/86400) as age_days;
filter_zero_sorted = ORDER filter_zero BY scope, name;
STORE filter_zero_sorted INTO '$OUT_FILE' USING PigStorage('\t');
