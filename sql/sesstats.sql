CREATE TABLE oradb.sesstats ( dbuniquename String, snapTime DateTime, sid UInt32, serial UInt32, statId UInt16, value Int64) ENGINE = MergeTree() partition by toYYYYMMDD(snapDate) order by(dbuniquename,snapTime)  SETTINGS index_granularity=8192;
CREATE TABLE oradb.sesstats_buffer AS oradb.sesstats ENGINE = Buffer('oradb', 'sesstats', 16, 14400, 86400, 100000, 1000000, 10000000, 100000000);
