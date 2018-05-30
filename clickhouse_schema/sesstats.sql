CREATE TABLE oradb.sesstats ( dbuniquename String,  snapTime DateTime, snapDate Date,  sid UInt32, statName String, classnum UInt8, value Int64 ) ENGINE = MergeTree(snapDate, dbuniquename, 1024);
CREATE TABLE oradb.sesstats_buffer AS oradb.sesstats ENGINE = Buffer(oradb, sesstats, 16, 14400, 86400, 100000, 1000000, 10000000, 100000000);
