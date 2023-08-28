import json
import os
from elasticsearch import Elasticsearch
from tqdm import tqdm
from dotenv import load_dotenv

from constants import JSON_DIR_PATH

def read_json_file_line_by_line(file_path):
    with open(file_path, "r") as file:
        for line in file:
            # 1行ずつJSONとして読み込む
            json_data = json.loads(line)

            # JSONデータを返す
            yield json_data

load_dotenv()
# Elasticsearchのインスタンスを初期化
es = Elasticsearch(
    [
        f"http://{os.getenv('TARGET_ELASTICSEARCH_HOST')}:{os.getenv('TARGET_ELASTICSEARCH_PORT')}"
    ],
    basic_auth=(
        os.getenv("TARGET_ELASTICSEARCH_USERNAME"),
        os.getenv("TARGET_ELASTICSEARCH_PASSWORD"),
    ),
)

# 指定したディレクトリのJSONファイルを繰り返し読み込む関数
def insert_json_from_directory(directory):
    json_files = [f for f in os.listdir(directory) if f.endswith('.json')]
    for filename in tqdm(json_files, desc="Processing JSON files"):
        if filename.endswith('.json'):
            # JSONファイル名 (拡張子を除く) をインデックス名として使用
            index_name = f"{filename[:-5]}"
            print(f"index_name: {index_name}")
            # ファイルからデータを読み込む
            data = list(read_json_file_line_by_line(os.path.join(directory, filename)))
            print(f"len(data): {len(data)}")
            # 各エントリーをElasticsearchにアップロード
            for entry in tqdm(data, desc=f"Uploading {filename}", leave=False):
                es.index(index=index_name, body=entry["_source"])

# JSONファイルからデータを読み込んでElasticsearchにアップロード
insert_json_from_directory(JSON_DIR_PATH)