import asyncio
from datetime import datetime
from elasticsearch import Elasticsearch, AsyncElasticsearch
from elasticsearch.helpers import async_streaming_bulk
import os

from dotenv import load_dotenv

load_dotenv()

# 非同期Elasticsearchクライアントのインスタンスを作成
async_es = AsyncElasticsearch(
    [
        f"http://{os.getenv('TARGET_ELASTICSEARCH_HOST')}:{os.getenv('TARGET_ELASTICSEARCH_PORT')}"
    ],
    basic_auth=(
        os.getenv("TARGET_ELASTICSEARCH_USERNAME"),
        os.getenv("TARGET_ELASTICSEARCH_PASSWORD"),
    ),
    request_timeout=60 * 60 * 24 * 7,
)

# 同期Elasticsearchクライアントのインスタンスを作成
es = Elasticsearch(
    [
        f"http://{os.getenv('TARGET_ELASTICSEARCH_HOST')}:{os.getenv('TARGET_ELASTICSEARCH_PORT')}"
    ],
    basic_auth=(
        os.getenv("TARGET_ELASTICSEARCH_SOFUE_WRITE_DATA_USERNAME"),
        os.getenv("TARGET_ELASTICSEARCH_SOFUE_WRITE_DATA_PASSWORD"),
    ),
    request_timeout=60 * 60 * 24 * 7,
)


async def create_index_and_insert_bulk(index_name, docs):
    # インデックスを作成
    if not es.indices.exists(index=index_name):
        es.indices.create(index=index_name)

    # async_streaming_bulkを使ってバルクインサート
    async for ok, response in async_streaming_bulk(async_es, docs):
        action, result = response.popitem()
        doc_id = result["_id"]
        if not ok:
            print(f"Failed to {action} document {doc_id}: {result}")
        else:
            print(f"Successfully {action} document {doc_id}")


async def main():
    # インデックス名とドキュメントデータ
    index_name = "sofue-test-index"

    docs = [
        {
            "_op_type": "create",
            "_index": index_name,
            "_source": {
                "timestamp": datetime.utcnow(),
                "message": f"message {i}"
            },
        }
        for i in range(10)
    ]

    # バルクインサート
    await create_index_and_insert_bulk(index_name, docs)

    # createメソッドを使って追加のドキュメントをインサート
    doc = {"timestamp": datetime.utcnow(), "message": "additional message"}
    res = es.index(index=index_name, body=doc)
    print("Created document:", res["_id"])

    # 非同期クライアントを閉じる
    await async_es.close()


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
