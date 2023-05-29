import os
from dotenv import load_dotenv
from elasticsearch import Elasticsearch


def delete_all_documents():
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
        request_timeout=60 * 60 * 24 * 7,
    )

    indices = ["2022_co2", "2023_co2"]
    delete_query = {"query": {"match_all": {}}}

    for index in indices:
        # Check if the index exists
        if es.indices.exists(index=index):
            # If the index exists, delete all documents in the index
            es.delete_by_query(
                index=index, body=delete_query, request_timeout=60 * 60 * 24 * 7
            )
            es.indices.delete(index=index)

            # インデックスの設定
            index_settings = {
                "settings": {
                    "number_of_shards": 4,  # プライマリシャードの数
                    "number_of_replicas": 1  # レプリカシャードの数
                }
            }
            # インデックスの作成
            es.indices.create(index=index, body=index_settings)
            print(f"インデックス{index}を初期化")
        else:
            print(f"インデックス{index}が存在しないのでスキップ")


if __name__ == "__main__":
    delete_all_documents()
