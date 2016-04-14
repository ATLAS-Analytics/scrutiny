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

traces = UNION traces1, traces2;

-- Drop unused columns

reduce_fields_dids = FOREACH dids GENERATE SCOPE as scope, NAME as name, TRANSIENT as transient, CREATED_AT/1000.0 as created_at;

filter_data_mc = FILTER reduce_fields_dids BY (transient IS NULL) AND (NOT (name MATCHES '.*_sub.*')) AND ((scope MATCHES 'data.*') OR (scope MATCHES 'mc.*') OR (scope MATCHES 'valid.*'));

reduce_fields_dids_2 = FOREACH filter_data_mc GENERATE scope, name, created_at;

-- RSEs with IDs
reduce_fields_rses = FOREACH rses GENERATE ID AS id, RSE AS rse;

reduce_fields_dslocks = FOREACH dslocks GENERATE SCOPE as scope, NAME as name, RSE_ID as rse_id, BYTES as bytes;

join_dslocks_rses = JOIN reduce_fields_dslocks BY rse_id, reduce_fields_rses BY id USING 'REPLICATED';

get_replicas = FOREACH join_dslocks_rses GENERATE reduce_fields_dslocks::scope as scope, reduce_fields_dslocks::name as name, reduce_fields_rses::rse as rse, (long)reduce_fields_dslocks::bytes as bytes;

get_datadisk_replicas = FILTER get_replicas BY (rse MATCHES '.*DATADISK');

join_dids_replicas = JOIN reduce_fields_dids_2 BY (scope, name), get_datadisk_replicas BY (scope, name);

get_primary_datadisk = FOREACH join_dids_replicas GENERATE get_datadisk_replicas::scope as scope, get_datadisk_replicas::name as name, get_datadisk_replicas::bytes as bytes, get_datadisk_replicas::rse as rse, reduce_fields_dids_2::created_at AS created_at;

group_primary_datadisk = GROUP get_primary_datadisk BY (scope, name, rse);

get_size_primary_datadisk = FOREACH group_primary_datadisk GENERATE group.scope as scope, group.name as name, group.rse as rse, MAX(get_primary_datadisk.bytes) as bytes, MIN(get_primary_datadisk.created_at) AS created_at;

filter_traces_time = FILTER traces BY timeentryunix IS NOT NULL;

group_traces = GROUP filter_traces_time BY (scope, name);
count_events = FOREACH group_traces GENERATE group.scope, group.name, COUNT(filter_traces_time.uuid) as op_events;

join_traces_primaries = JOIN get_size_primary_datadisk BY (scope, name) LEFT OUTER, count_events BY (scope, name);

get_event_count = FOREACH join_traces_primaries GENERATE get_size_primary_datadisk::scope, get_size_primary_datadisk::created_at AS created_at, get_size_primary_datadisk::name as name, get_size_primary_datadisk::rse as rse, get_size_primary_datadisk::bytes AS bytes, ((count_events::op_events IS NULL) ? 0 : count_events::op_events) AS op_events;

group_scope_name = GROUP get_event_count BY (scope, name);

get_dataset_volume = FOREACH group_scope_name GENERATE group.scope as scope, group.name as name, MIN(get_event_count.created_at) AS created_at, MAX(get_event_count.op_events) AS op_events, SUM(get_event_count.bytes) AS bytes;

filter_zero_ = FILTER get_dataset_volume BY op_events == 0;
filter_zero = FOREACH filter_zero_ GENERATE scope, name, bytes, (int)created_at, (int)(($TIME_UTC - created_at)/86400) as age_days;
filter_zero_sorted = ORDER filter_zero BY scope, name;
STORE filter_zero_sorted INTO '$OUT_FILE' USING PigStorage('\t');

