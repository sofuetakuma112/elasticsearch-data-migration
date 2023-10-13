source .env

multielasticdump \
    --match='co2' \
    --input=http://$TARGET_ELASTICSEARCH_USERNAME:$TARGET_ELASTICSEARCH_PASSWORD@$TARGET_ELASTICSEARCH_HOST:$TARGET_ELASTICSEARCH_PORT \
    --output=/media/sofue/C45ACC905ACC8122/133.71.106.141_elasticsearch_data/co2/jsons_missing_jptime_docs \
    --limit=10000 \
    --scrollTime='10m' \
    --ignoreType='mapping,analyzer,alias,settings,template' \
    --searchBody='{
        "query": {
            "bool": {
                "must": [
                    {"range": {"utctime": {"gte": "2023-01-01T00:00:00Z"}}}
                ],
                "must_not": [
                    {"exists": {"field": "JPtime"}}
                ]
            }
        }
    }'
