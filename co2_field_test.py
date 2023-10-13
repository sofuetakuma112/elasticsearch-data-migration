import os
from elasticsearch import Elasticsearch

from dotenv import load_dotenv

load_dotenv()

# Elasticsearch インスタンスを作成
es = Elasticsearch(
    [
        f"http://{os.getenv('TARGET_ELASTICSEARCH_HOST')}:{os.getenv('TARGET_ELASTICSEARCH_PORT')}"
    ],
    basic_auth=(
        os.getenv("TARGET_ELASTICSEARCH_USERNAME"),
        os.getenv("TARGET_ELASTICSEARCH_PASSWORD"),
    ),
    request_timeout=60 * 60 * 24 * 7,
)

# インデックス名とドキュメントタイプを指定
index_name = "co2_field_test"

# インサートするドキュメントデータ
doc_data = {"PPM": 1000.5}  # ここでPPMの値をセット

# ドキュメントをインデックスにインサート
response = es.index(index=index_name, body=doc_data)

# レスポンスを表示
print(response)
