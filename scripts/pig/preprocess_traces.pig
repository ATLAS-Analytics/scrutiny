-- Preprocessing traces: join with Rucio's contents

SET job.name atlas-ddm-scrutiny-preprocess-traces;

REGISTER '/usr/lib/pig/piggybank.jar';
REGISTER '/usr/lib/avro/avro.jar';
REGISTER '/usr/lib/avro/avro-mapred.jar';
REGISTER '/usr/lib/pig/lib/json-simple-1.1.jar';

REGISTER '/afs/cern.ch/user/a/atlstats/public/scrutiny/lib/rucioudfs.jar';

DEFINE ToISO org.apache.pig.piggybank.evaluation.datetime.convert.CustomFormatToISO();
DEFINE ToUnix org.apache.pig.piggybank.evaluation.datetime.convert.ISOToUnix();

contents = LOAD '/user/rucio01/dumps/$DATE/contents' USING AvroStorage();

-- Preprocessed traces for the year 2015 are already in HDFS
/*
rucio_traces15 = LOAD '/user/rucio01/traces/traces.2015-{08,09,10,11,12}-*[0-9]' USING rucioudfs.TracesLoader() as (
  timeentry: double,
  timeStart: double,
  timeEnd: double,
  dataset:chararray,
  filename:chararray,
  hostname:chararray,
  scope:chararray,
  localSite:chararray,
  remoteSite:chararray,
  ip:chararray,
  eventType:chararray,
  clientState:chararray,
  uuid:chararray,
  usrdn:chararray
);
*/

rucio_traces16 = LOAD '/user/rucio01/traces/traces.2016-*[0-9]' USING rucioudfs.TracesLoader() as (
  timeentry: double,
  timeStart: double,
  timeEnd: double,
  dataset:chararray,
  filename:chararray,
  hostname:chararray,
  scope:chararray,
  localSite:chararray,
  remoteSite:chararray,
  ip:chararray,
  eventType:chararray,
  clientState:chararray,
  uuid:chararray,
  usrdn:chararray
);

-- rucio_traces = UNION rucio_traces15, rucio_traces16;
rucio_traces = rucio_traces16;

traces = FOREACH rucio_traces GENERATE dataset, timeentry as timeentryunix, eventType as eventtype, usrdn, remoteSite as remotesite, localSite as localsite, uuid, filename;

traces_valid = FILTER traces BY (filename IS NOT NULL) AND (eventtype IS NOT NULL) and (timeentryunix is not null);

content = FOREACH contents GENERATE SCOPE as scope, NAME as name, CHILD_NAME as child_name, DID_TYPE as did_type, CHILD_TYPE as child_type;

D1 = FILTER content BY did_type == 'D' and child_type == 'F';

D2 = JOIN traces_valid BY (filename), D1 BY (child_name);

D = FOREACH D2 GENERATE D1::scope as scope, D1::name as name, traces_valid::eventtype as eventtype, traces_valid::uuid as uuid, traces_valid::timeentryunix;

-- Filtering traces by access operation and storage type
DESCRIBE D;
E = FILTER D BY ((scope MATCHES 'data.*') OR (scope MATCHES 'mc.*') OR (scope MATCHES 'valid.*')) AND (eventtype MATCHES 'get.*' OR eventtype MATCHES 'sm_get.*'  OR eventtype == 'download');

F = GROUP E BY (scope, name, uuid);

G = FOREACH F GENERATE group.scope as scope, group.name as name, group.uuid as uuid, COUNT(E) as fileops, MAX(E.timeentryunix) as timeentry;

H = ORDER G BY timeentry ASC;

STORE H INTO '$OUT_FILE' USING PigStorage('\t');
