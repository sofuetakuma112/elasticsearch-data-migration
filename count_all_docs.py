import os
from dotenv import load_dotenv
from elasticsearch import Elasticsearch

# Elasticsearchの接続情報を設定する
load_dotenv()
es = Elasticsearch(
    [
        f"http://{os.getenv('TARGET_ELASTICSEARCH_HOST')}:{os.getenv('TARGET_ELASTICSEARCH_PORT')}"
    ],
    basic_auth=(
        os.getenv("TARGET_ELASTICSEARCH_USERNAME"),
        os.getenv("TARGET_ELASTICSEARCH_PASSWORD"),
    ),
)
# '2023_co2'インデックスのドキュメント数をカウントする
index_name = '2023_co2'
count = es.count(index=index_name)['count']
print(f"ドキュメント数: {count}")

# Elasticsearchとの接続を閉じる
es.close()