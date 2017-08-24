# OraPerf2Elasticsearch
Quite raw, to be improved.

Gather waits and events from Oracle Database on regular basis and put it in elasticsearch.

Sample elasticsearch index template:

{
  "order": 0,
  "template": "grid_*",
  "settings": {
    "index": {
      "number_of_shards": "1",
      "number_of_replicas": "0"
    }
  },
  "mappings": {
    "events": {
      "properties": {
        "SnapTime": {
          "format": "dd.MM.YYYY HH:mm:ss",
          "index": true,
          "store": true,
          "type": "date"
        },
        "Database": {
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
          "long2short": {
            "mapping": {
              "index": true,
              "store": true,
              "type": "short"
            },
            "match_mapping_type": "long"
          }
        },
        {
          "waitclass": {
            "path_match": "Waits.WaitClass",
            "mapping": {
              "index": true,
              "store": true,
              "type": "keyword"
            }
          }
        },
        {
          "eventname": {
            "path_match": "Events.EventName",
            "mapping": {
              "index": true,
              "store": true,
              "type": "keyword"
            }
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
    }
  },
  "aliases": {}
}

record format example for db.lst file:

hostname1.example.net:1521/db_service_name

