# 修論の3.5節で使用したプログラム
# 移行元のElasticsearchのCO2データをエクスポートしている

source .env

multielasticdump \
    --match='.*co2.*' \
    --input=http://$SOURCE_ELASTICSEARCH_USERNAME:$SOURCE_ELASTICSEARCH_PASSWORD@$SOURCE_ELASTICSEARCH_HOST:$SOURCE_ELASTICSEARCH_PORT \
    --output=/media/sofue/C45ACC905ACC8122/old_elasticsearch_data/jsons \
    --limit=10000 \
    --scrollTime='10m' \
    --ignoreType='mapping,analyzer,alias,settings,template'