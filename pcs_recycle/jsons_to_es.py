import json
import os
from elasticsearch import AsyncElasticsearch
from elasticsearch.helpers import async_streaming_bulk, BulkIndexError
from tqdm import tqdm
from dotenv import load_dotenv
import asyncio
import logging

# ロギングの設定
logging.basicConfig(filename="out/error.log", level=logging.ERROR)


async def generate_bulk_data(docs):
    for doc in tqdm(docs, desc="Processing"):
        index_name = "pcs_recyclekan"
        source = doc["_source"]
        yield {"_op_type": "create", "_index": index_name, "_source": source}


async def main():
    """
    JSONデータを指定したindex名のインデックスに保存する関数。

    :param data: JSONデータのリスト
    :param index_name: 保存するindex名
    """

    # Elasticsearchの接続情報を設定する
    load_dotenv()
    es = AsyncElasticsearch(
        [
            f"http://{os.getenv('TARGET_ELASTICSEARCH_HOST')}:{os.getenv('TARGET_ELASTICSEARCH_PORT')}"
        ],
        basic_auth=(
            os.getenv("TARGET_ELASTICSEARCH_USERNAME"),
            os.getenv("TARGET_ELASTICSEARCH_PASSWORD"),
        ),
        request_timeout=60 * 60 * 24 * 7,
    )

    JSON_PATH = "/media/sofue/C45ACC905ACC8122/pcs_recycle/filtered.json"
    docs = []

    with open(JSON_PATH, "r") as file:
        docs = json.load(file)

    # Elasticsearchへのデータの追加
    bulk_data = generate_bulk_data(docs)

    try:
        # ドキュメントを複数（チャンク）に分けてバルクインサート
        async for ok, result in async_streaming_bulk(
            client=es,
            actions=bulk_data,
            chunk_size=100,
        ):
            # 各チャンクごとの実行結果を取得
            action, result = result.popitem()
            # バルクインサートに失敗した場合
            if not ok:
                logging.error(f"failed to {result} document {action}")
    except BulkIndexError as bulk_error:
        # エラーはリスト形式
        logging.error(bulk_error.errors)

    await es.close()


if __name__ == "__main__":
    # イベントループを取得
    loop = asyncio.get_event_loop()
    # 並列に実行して終るまで待つ
    loop.run_until_complete(main())
