import json
import os
import psutil
from elasticsearch import Elasticsearch
from dotenv import load_dotenv
import concurrent
from tqdm import tqdm

from constants import JSON_DIR_FULL_PATH

load_dotenv()
es = Elasticsearch(
    [
        f"http://{os.getenv('SOURCE_ELASTICSEARCH_HOST')}:{os.getenv('SOURCE_ELASTICSEARCH_PORT')}"
    ],
    basic_auth=(
        os.getenv("SOURCE_ELASTICSEARCH_USERNAME"),
        os.getenv("SOURCE_ELASTICSEARCH_PASSWORD"),
    ),
)


def estimate_json_size(index_name: str) -> float:
    # インデックスの統計情報を取得
    stats = es.indices.stats(index=index_name)

    # インデックスのサイズを取得（バイト単位）
    size_in_bytes = stats["indices"][index_name]["total"]["store"]["size_in_bytes"]

    # サイズをGBに変換
    size_in_gb = size_in_bytes / (1024**3)

    return size_in_gb


def file_exists(dir_path, file_name):
    for f in os.listdir(dir_path):
        if f == file_name:
            return True
    return False


def process_index(index):
    json_file_names = list(
        map(
            lambda file_name_with_ext: os.path.splitext(file_name_with_ext)[0],
            os.listdir(JSON_DIR_FULL_PATH),
        )
    )
    if index in json_file_names:
        return
    print(f"now processing {index} ...")

    size_in_gb = estimate_json_size(index)
    print(f"{index}の推定サイズ: {size_in_gb} GB")

    s_time = "2m"
    data = es.search(
        index=index,
        scroll=s_time,
        body={"query": {"match_all": {}}},
        size=1000,
        request_timeout=150,
    )

    s_id = data["_scroll_id"]
    s_size = data["hits"]["total"]["value"]

    total_iterations = (s_size // 1000) + 1

    with open(f"{JSON_DIR_FULL_PATH}/{index}.json", "w") as f:
        with tqdm(total=total_iterations) as pbar:
            while s_size > 0:
                documents = data["hits"]["hits"]
                json.dump(documents, f, ensure_ascii=False, indent=4)
                f.write("\n")

                data = es.scroll(scroll_id=s_id, scroll=s_time, request_timeout=150)
                s_id = data["_scroll_id"]
                s_size = len(data["hits"]["hits"])

                pbar.update(1)

    print(f"{index}のダンプ終了")


indices = es.cat.indices(v=True).strip().split("\n")
index_list = [index.split()[2] for index in indices]

# Create the "jsons" directory if it doesn't exist
if not os.path.exists(JSON_DIR_FULL_PATH):
    os.mkdir(JSON_DIR_FULL_PATH)

# Determine the maximum number of threads based on the CPU core count
max_threads = psutil.cpu_count(logical=True)

# 並行実行のためのスレッドプールを作成
with concurrent.futures.ThreadPoolExecutor(max_workers=max_threads) as executor:
    # 各indexの処理を並行実行
    futures = [
        executor.submit(process_index, index) for index in index_list if "co2" in index
    ]

    # 完了したタスクの結果を取得
    for future in concurrent.futures.as_completed(futures):
        # エラーが発生した場合には例外を取得するなど、必要な処理を実装
        result = future.result()
