from dotenv import load_dotenv
from elasticsearch import Elasticsearch, helpers
import os
from tqdm import tqdm
from util import read_json_file_line_by_line, get_json_file_line_count

load_dotenv()


# Elasticsearchクライアントのセットアップ
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

# ダンプされたJSONのファイルが保存されているディレクトリ
DIRECTORY_PATH = "/media/sofue/C45ACC905ACC8122/133.71.106.141_elasticsearch_data/co2/jsons_missing_jptime_docs"

for filename in os.listdir(DIRECTORY_PATH):
    if filename.endswith(".json"):
        file_path = os.path.join(DIRECTORY_PATH, filename)
        file_lines = read_json_file_line_by_line(file_path)
        total_lines = get_json_file_line_count(file_path)

        bulk_actions = []  # For bulk indexing

        for entry in tqdm(
            file_lines,
            total=total_lines,
            desc=filename,
            unit="line",
        ):
            # 新しくインサートするデータ
            doc_to_insert = entry["_source"]

            # utctimeとnumberの組み合わせで検索を実行
            query = {
                "query": {
                    "bool": {
                        "must": [
                            {"match": {"number": doc_to_insert["number"]}},
                            {"match": {"utctime": doc_to_insert["utctime"]}},
                        ]
                    }
                }
            }

            results = es.search(index="co2_modbus", body=query)

            # 重複チェック
            if results["hits"]["total"]["value"] == 0:
                action = {
                    "_op_type": "index",
                    "_index": "co2_modbus",
                    "_type": "_doc",
                    "_source": doc_to_insert,
                }
                bulk_actions.append(action)

        if bulk_actions:
            helpers.bulk(es, bulk_actions)
            print(f"{len(bulk_actions)} documents inserted!")
