curl -X PUT "localhost:9200/opensearch-sql_test_index_account" -H "Content-Type: application/json" -d '
{
  "settings": {
    "index": {
      "number_of_shards": 2,
      "number_of_replicas": 1
    }
  },
  "mappings": {
    "properties": {
      "gender": {
        "type": "text",
        "fielddata": true,
        "fields": {
          "keyword": {
            "type": "keyword",
            "ignore_above": 256
          }
        }
      },
      "address": {
        "type": "text",
        "fielddata": true
      },
      "firstname": {
        "type": "text",
        "fielddata": true,
        "fields": {
          "keyword": {
            "type": "keyword",
            "ignore_above": 256
          }
        }
      },
      "lastname": {
        "type": "text",
        "fielddata": true,
        "fields": {
          "keyword": {
            "type": "keyword",
            "ignore_above": 256
          }
        }
      },
      "state": {
        "type": "text",
        "fielddata": true,
        "fields": {
          "keyword": {
            "type": "keyword",
            "ignore_above": 256
          }
        }
      }
    }
  }

}'

curl -H "Content-Type: application/x-ndjson" -POST "http://localhost:9200/opensearch-sql_test_index_account/_bulk" -u 'admin:admin' --insecure --data-binary "@./integ-test/src/test/resources/accounts.json"

