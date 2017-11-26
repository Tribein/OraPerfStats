Quite raw, to be improved.

Gather waits and events from Oracle Database on regular basis and put it in elasticsearch/clickhouse.

record format example for db.lst file:

hostname1.example.net:1521/db_service_name

=======
ClickHouse:
CREATE TABLE ${DATABASE}.waitclass ( classnum UInt8,  class String) ENGINE = TinyLog;


cat << EOF | clickhouse-client  -h $DBHOST -u $USERNAME --password $PASSWORD -d $DATABASE --query="INSERT INTO 
${DATABASE}.waitclass FORMAT TSV";
127     CPU
0       Other
1       Application
2       Configuration
3       Administrative
4       Concurrency
5       Commit
6       Idle
7       Network
8       User I/O
9       System I/O
10      Scheduler
12      Queueing
EOF


CREATE TABLE ${DATABASE}.sessions ( dbuniquename String,  hostname String,  snapTime DateTime,  sid UInt32,  serial UInt32,  opentrn FixedString(1),  status FixedString(1),  schemaname String,  osuser String,  machine String,  program String,  type FixedString(1),  module String,  blocking_session UInt32,  event String,  classnum UInt8,  wait_time Float32,  sql_id FixedString(13),  sql_exec_start DateTime,  sql_exec_id UInt32,  logon_time DateTime,  seq UInt32,  snapDate Date,  p1 UInt64,  p2 UInt64) ENGINE = MergeTree(snapDate, dbuniquename, 1024);

CREATE TABLE ${DATABASE}.sessions_buffer AS ${DATABASE}.sessions ENGINE = Buffer(${DATABASE}, sessions, 16, 14400, 86400, 100000, 1000000, 100000000, 1000000000);

CREATE TABLE ${DATABASE}.sysstats ( dbuniquename String,  snapTime DateTime, snapDate Date,  statName String, classnum UInt8, value Int64 ) ENGINE = MergeTree(snapDate, dbuniquename, 1024);

CREATE TABLE ${DATABASE}.sysstats_buffer AS ${DATABASE}.sysstats ENGINE = Buffer(${DATABASE}, sysstats, 16, 14400, 86400, 100000, 1000000, 10000000, 100000000);

=======
!!!Elasticsearch as database for this kind of stats is found not very handy, so it's supported in case...

Sample elasticsearch index template:

{
  "order": 0,
  "template": "grid_*",
  "settings": {
    "index": {
      "number_of_shards": "1",
      "number_of_replicas": "0",
      "mapping": {
        "total_fields": {
          "limit": "1048576"
        }
      }
    }
  },
  "mappings": {
    "_default_": {
      "_source": {
        "enabled": false
      },
      "dynamic_templates": [
        {
          "string2kw": {
            "mapping": {
              "index": true,
              "store": true,
              "type": "keyword"
            },
            "match_mapping_type": "string"
          }
        },
        {
          "logontimemilli": {
            "mapping": {
              "format": "epoch_millis",
              "index": true,
              "store": true,
              "type": "date"
            },
            "match_mapping_type": "long",
            "match": "LogonTime"
          }
        },
        {
          "sqlexecstartmilli": {
            "mapping": {
              "format": "epoch_millis",
              "index": true,
              "store": true,
              "type": "date"
            },
            "match_mapping_type": "long",
            "match": "SQLExecStart"
          }
        },
        {
          "sesslong": {
            "mapping": {
              "index": true,
              "store": true,
              "type": "long"
            },
            "match_mapping_type": "long"
          }
        }
      ],
      "_all": {
        "enabled": false
      }
    },
    "sessions": {
      "properties": {
        "SnapTime": {
          "format": "dd.MM.YYYY HH:mm:ss",
          "index": true,
          "store": true,
          "type": "date"
        },
        "Transaction": {
          "index": true,
          "store": true,
          "type": "boolean"
        },
        "Database": {
          "index": true,
          "store": true,
          "type": "keyword"
        },
        "Hostname": {
          "index": true,
          "store": true,
          "type": "keyword"
        },
        "WaitTime": {
          "index": true,
          "store": true,
          "type": "float"
        }
      }
    }
  },
  "aliases": {}
}
