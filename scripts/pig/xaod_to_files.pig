SET job.name atlas-ddm-scrutiny-xaod-traces-to-files;
REGISTER '/usr/lib/pig/lib/json-simple-*.jar';
REGISTER '$LIB_DIR/elephant-bird-core-4.9.jar';
REGISTER '$LIB_DIR/elephant-bird-hadoop-compat-4.9.jar';
REGISTER '$LIB_DIR/elephant-bird-pig-4.9.jar';
REGISTER '$LIB_DIR/resolve_udfs.py' USING jython AS udfs;

/*
This script is used to convert the nested XAOD JSON traces into a simple TSV format
with one trace per file, which then can be used as input for the actual processing
of AOD/DAOD accesses.
*/

traces = LOAD '$HDFS_RUCIO_DIR/nongrid_traces/{$MONTH}-*.json' using com.twitter.elephantbird.pig.load.JsonLoader('-nestedLoad') AS (json:map[]);

add_uuid = FOREACH traces GENERATE $0#'timeentry' as timeentry, $0#'PandaID' as id, $0#'accessedFiles' as files, udfs.UUID() as uuid, $0#'ReportRate' as rate;

flatten_all = FOREACH add_uuid GENERATE timeentry, uuid, FLATTEN(id) as id, FLATTEN(files) as file, rate;

order_all = ORDER flatten_all BY timeentry ASC, uuid ASC;

STORE order_all INTO '$HDFS_XAOD_TRACES_DIR/$MONTH' USING PigStorage('\t');
