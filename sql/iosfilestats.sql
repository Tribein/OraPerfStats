CREATE TABLE oradb.iofilestats ( dbuniquename String, snapTime DateTime, fileType String, fileName String, srmb UInt64, swmb UInt64, lrmb UInt64, lwmb UInt64, srrq UInt64, swrq UInt64, lrrq UInt64, lwrq UInt64, ssyncrrq UInt64, srsvtm UInt64, swsvtm UInt64, ssyncrlt UInt64, lrsvtm UInt64, lwsvtm UInt64 ) ENGINE = MergeTree() partition by toYYYYMM(snapTime) order by (dbuniquename,snapTime);
CREATE TABLE oradb.iofilestats_buffer AS oradb.iofilestats ENGINE = Buffer('oradb', 'iofilestats', 16, 14400, 86400, 100000, 1000000, 10000000, 100000000);
