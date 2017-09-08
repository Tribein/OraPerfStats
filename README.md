Quite raw, to be improved.

Gather waits and events from Oracle Database on regular basis and put it in elasticsearch.

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
    "sessions": {
      "properties": {
        "SnapTime": {
          "format": "dd.MM.YYYY HH:mm:ss",
          "index": true,
          "store": true,
          "type": "date"
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
        }
      }
    },
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
    }
  },
  "aliases": {}
}

record format example for db.lst file:

hostname1.example.net:1521/db_service_name

=======
ClickHouseDb:
CREATE TABLE orasessions ( dbuniquename String,  hostname String,  snapTime DateTime,  sid UInt32,  serial UInt32,  opentrn String,  status String,  schemaname String,  osuser String,  machine String,  program String,  type String,  module String,  blocking_session UInt32,  event String,  class String,  wait_time Float32,  sql_id String,  sql_exec_start DateTime,  sql_exec_id UInt32,  logon_time DateTime,  seq UInt32) ENGINE = Log;
