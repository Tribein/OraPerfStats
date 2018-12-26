CREATE TABLE oradb.sqlplans ( sqlid FixedString(13),  planhashvalue UInt64 ) ENGINE = ReplacingMergeTree() order by sqlid;
CREATE TABLE oradb.sqlplans_buffer as oradb.sqlplans ENGINE = Buffer('oradb', 'sqlplans', 16, 900, 14400, 1000000, 10000000, 10000000, 1000000000);
