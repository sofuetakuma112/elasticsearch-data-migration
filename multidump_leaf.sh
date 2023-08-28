source .env

multielasticdump \
    --match='leaf' \
    --input=http://$PCS_RECYCLE_ELASTICSEARCH_USERNAME:$PCS_RECYCLE_ELASTICSEARCH_PASSWORD@$PCS_RECYCLE_ELASTICSEARCH_HOST:$PCS_RECYCLE_ELASTICSEARCH_PORT \
    --output=/media/sofue/C45ACC905ACC8122/133.71.201.197/leaf/dump \
    --limit=10000 \
    --scrollTime='10m' \
    --ignoreType='mapping,analyzer,alias,settings,template'