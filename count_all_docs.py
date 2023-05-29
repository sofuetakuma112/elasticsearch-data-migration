import os
from dotenv import load_dotenv
from elasticsearch import Elasticsearch


def count_all_documents():
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

    for index in indices:
        # Check if the index exists
        if es.indices.exists(index=index):
            count = es.count(index=index)["count"]
            print(f"index: {index} => ドキュメント数: {count}")
        else:
            print(f"Index {index} does not exist. Skipping deletion.")


if __name__ == "__main__":
    count_all_documents()
