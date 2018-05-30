CREATE TABLE oradb.sysstats ( dbuniquename String,  snapTime DateTime, snapDate Date,  statName String, classnum UInt8, value Int64 ) ENGINE = MergeTree(snapDate, dbuniquename, 1024);
CREATE TABLE oradb.sysstats_buffer AS oradb.sysstats ENGINE = Buffer(oradb, sysstats, 16, 14400, 86400, 100000, 1000000, 10000000, 100000000);
