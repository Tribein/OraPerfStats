CREATE TABLE oradb.statnames ( dbuniquename String,  statId UInt16,  statName String) ENGINE = ReplacingMergeTree() partition by dbuniquename order by statId;
CREATE TABLE oradb.statnames_buffer AS oradb.statnames ENGINE = Buffer('oradb', 'statnames', 16, 900, 3600, 100000, 1000000, 1000000, 100000000);
