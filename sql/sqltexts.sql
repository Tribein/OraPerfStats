CREATE TABLE oradb.sqltexts ( sqlid FixedString(13),  sqltext String) ENGINE = ReplacingMergeTree() order by sqlid;
CREATE TABLE oradb.sqltexts_buffer as oradb.sqltexts ENGINE = Buffer('oradb', 'sqltexts', 16, 900, 14400, 1000000, 10000000, 10000000, 1000000000);
