CREATE TABLE oradb.sysstats ( dbuniquename String, snapTime DateTime, statId UInt16, value Int64 ) ENGINE = MergeTree() partition by toYYYYMMDD(snapDate) order by (dbuniquename,snapTime) SETTINGS index_granularity=8192;
CREATE TABLE oradb.sysstats_buffer AS oradb.sysstats ENGINE = Buffer('oradb', 'sysstats', 16, 14400, 86400, 100000, 1000000, 10000000, 100000000);
