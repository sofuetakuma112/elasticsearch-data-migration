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
    )

    # 2022_co2インデックスのドキュメントを削除
    # delete_query_2022 = {"query": {"match_all": {}}}
    # es.delete_by_query(
    #     index="2022_co2", body=delete_query_2022, request_timeout=60 * 60 * 24 * 7
    # )

    # 2023_co2インデックスのドキュメントを削除
    delete_query_2023 = {"query": {"match_all": {}}}
    print(delete_query_2023)
    es.delete_by_query(
        index="2023_co2", body=delete_query_2023, timeout=60 * 60 * 24 * 7
    )

    print("All documents deleted successfully.")


if __name__ == "__main__":
    delete_all_documents()
