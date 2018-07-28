CREATE TABLE oradb.sesstats ( dbuniquename String,  snapDate Date, snapTime DateTime, sid UInt32, serial UInt32, statId UInt16, value Int64) ENGINE = MergeTree(snapDate, dbuniquename, 1024);
CREATE TABLE oradb.sesstats_buffer AS oradb.sesstats ENGINE = Buffer(oradb, sesstats, 16, 14400, 86400, 100000, 1000000, 10000000, 100000000);
