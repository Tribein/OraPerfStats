# OraPerf2Elasticsearch
Quite raw, to be improved.

Gather waits and events from Oracle Database on regular basis and put it in elasticsearch.

Sample elasticsearch index template:

{
  "order": 0,
  "template": "grid",
  "settings": {
    "index": {
      "number_of_shards": "1"
    }
  },
  "mappings": {
    "events": {
      "properties": {
        "SnapTime": {
          "format": "dd.MM.YYYY HH:mm:ss",
          "store": true,
          "type": "date"
        },
        "Database": {
          "index": false,
          "store": true,
          "type": "text"
        },
        "Hostname": {
          "index": false,
          "store": true,
          "type": "text"
        }
      }
    },
    "_default_": {
      "_source": {
        "enabled": false
      },
      "dynamic_templates": [
        {
          "longs": {
            "mapping": {
              "store": true,
              "type": "short"
            },
            "match_mapping_type": "long"
          }
        }
      ],
      "_all": {
        "enabled": false
      }
    },
    "waits": {
      "properties": {
        "SnapTime": {
          "format": "dd.MM.YYYY HH:mm:ss",
          "store": true,
          "type": "date"
        },
        "Database": {
          "index": false,
          "store": true,
          "type": "text"
        },
        "Hostname": {
          "index": false,
          "store": true,
          "type": "text"
        }
      }
    }
  },
  "aliases": {}
}

record format example for db.lst file:

hostname1.example.net:1521/db_service_name

