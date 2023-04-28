import json
import os
from elasticsearch import Elasticsearch
from dotenv import load_dotenv

load_dotenv()

es = Elasticsearch([f"http://{os.getenv('SOURCE_ELASTICSEARCH_HOST')}:{os.getenv('SOURCE_ELASTICSEARCH_PORT')}"], 
                   http_auth=(os.getenv('SOURCE_ELASTICSEARCH_USERNAME'), os.getenv('SOURCE_ELASTICSEARCH_PASSWORD')))

indices = es.cat.indices(v=True).strip().split("\n")
index_list = [index.split()[2] for index in indices]

# jsonsディレクトリが存在しない場合は作成する
if not os.path.exists("jsons"):
    os.mkdir("jsons")

for index in index_list:
    if "co2" in index:
        # スクロールAPIを使用してインデックスからすべてのドキュメントを取得
        scroll = es.search(index=index, scroll="1m", body={"query": {"match_all": {}}})

        # スクロールAPIから取得したドキュメントを格納するリスト
        documents = []

        # スクロールAPIからすべてのドキュメントを取得する
        while scroll["hits"]["hits"]:
            documents += scroll["hits"]["hits"]
            scroll = es.scroll(scroll_id=scroll["_scroll_id"], scroll="1m")

        # 取得したドキュメントをインデックス名のJSONファイルに書き込む
        with open(f"jsons/{index}.json", "w") as f:
            json.dump(documents, f, ensure_ascii=False, indent=4)
