CREATE TABLE oradb.statnames ( snapDate Date,  snapTime DateTime,  dbuniquename String,  statId UInt16,  statName String) ENGINE = ReplacingMergeTree(snapDate, (dbuniquename, statId), 8192);
CREATE TABLE oradb.statnames_buffer AS oradb.statnames ENGINE = Buffer('oradb', 'statnames', 16, 900, 3600, 100000, 1000000, 1000000, 100000000);
