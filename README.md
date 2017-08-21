# OraPerf2Elasticsearch
Quite raw, to be improved.

Gather waits and events from Oracle Database on regular basis and put it in elasticsearch.

Sample elasticsearch index template:

{
  "order": 0,
  "template": "grid",
  "settings": {},
  "mappings": {
    "events": {
      "properties": {
        "SnapTime": {
          "format": "dd.MM.YYYY HH:mm:ss",
          "type": "date"
        },
        "Database": {
          "index": "false",
          "type": "text"
        }
      }
    },
    "waits": {
      "properties": {
        "SnapTime": {
          "format": "dd.MM.YYYY HH:mm:ss",
          "type": "date"
        },
        "Database": {
          "index": "false",
          "type": "text"
        }
      }
    }
  },
  "aliases": {}
}

record format example for db.lst file:

hostname1.example.net:1521/db_service_name

