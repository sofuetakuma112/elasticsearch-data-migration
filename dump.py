import json
import os
from elasticsearch import Elasticsearch
from dotenv import load_dotenv

load_dotenv()

es = Elasticsearch(
    [
        f"http://{os.getenv('SOURCE_ELASTICSEARCH_HOST')}:{os.getenv('SOURCE_ELASTICSEARCH_PORT')}"
    ],
    http_auth=(
        os.getenv("SOURCE_ELASTICSEARCH_USERNAME"),
        os.getenv("SOURCE_ELASTICSEARCH_PASSWORD"),
    ),
)

indices = es.cat.indices(v=True).strip().split("\n")
index_list = [index.split()[2] for index in indices]

# jsonsディレクトリが存在しない場合は作成する
if not os.path.exists("jsons"):
    os.mkdir("jsons")

for index in index_list:
    if "co2" in index:
        print(f"index: {index}")
        
        s_time = "2m"
        data = es.search(
            index=index,
            scroll=s_time,
            body={"query": {"match_all": {}}},
            size=1000,
            request_timeout=150,
        )

        s_id = data["_scroll_id"]
        s_size = data["hits"]["total"]["value"]  # 残りの検索対象の件数??
        documents = data["hits"]["hits"]
        while s_size > 0:
            data = es.scroll(
                scroll_id=s_id, scroll=s_time, request_timeout=150
            )  # scroll: スクロール時の検索コンテキストを保持するための期間
            s_id = data["_scroll_id"]
            s_size = len(data["hits"]["hits"])
            documents.extend(data["hits"]["hits"])

        # 取得したドキュメントをインデックス名のJSONファイルに書き込む
        with open(f"jsons/{index}.json", "w") as f:
            json.dump(documents, f, ensure_ascii=False, indent=4)
