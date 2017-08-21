# OraPerf2Elasticsearch
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
          "type": "string"
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
          "type": "string"
        }
      }
    }
  },
  "aliases": {}
}

record format example for db.lst file:
hostname1.example.net:1521/db_service_name
hostname2.example.net:1521/db_service_name
...
