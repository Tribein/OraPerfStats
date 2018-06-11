CREATE TABLE oradb.sqltexts ( snapDate Date,  sqlid FixedString(13),  sqltext String,  snapTime DateTime) ENGINE = ReplacingMergeTree(snapDate, sqlid, 8192)
CREATE TABLE oradb.sqltexts_buffer ( snapDate Date,  sqlid FixedString(13),  sqltext String,  snapTime DateTime) ENGINE = Buffer('oradb', 'sqltexts', 16, 900, 14400, 1000000, 10000000, 10000000, 1000000000)
