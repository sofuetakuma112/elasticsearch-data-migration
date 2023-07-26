source .env

multielasticdump \
    --match='.*co2.*' \
    --input=http://$SOURCE_ELASTICSEARCH_USERNAME:$SOURCE_ELASTICSEARCH_PASSWORD@$SOURCE_ELASTICSEARCH_HOST:$SOURCE_ELASTICSEARCH_PORT \
    --output=/media/sofue/C45ACC905ACC8122/old_elasticsearch_data/jsons_after_20230501 \
    --limit=10000 \
    --scrollTime='10m' \
    --ignoreType='mapping,analyzer,alias,settings,template' \
    --searchBody='{"query": {"range": {"utctime": {"gte": "2023-05-01T00:00:00Z"}}}}'
