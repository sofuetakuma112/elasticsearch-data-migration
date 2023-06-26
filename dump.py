import argparse
import json
import os
from elasticsearch import Elasticsearch
from dotenv import load_dotenv
import concurrent
from tqdm import tqdm

from utils.constants import OTHER_JSON_DIR_FULL_PATH


def process_index(index):
    json_file_names = list(
        map(
            lambda file_name_with_ext: os.path.splitext(file_name_with_ext)[0],
            os.listdir(OTHER_JSON_DIR_FULL_PATH),
        )
    )
    if index in json_file_names:
        return

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

    with open(f"{OTHER_JSON_DIR_FULL_PATH}/{index}.json", "w") as f:
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


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Dump Elasticsearch index to a JSON file.')
    parser.add_argument('index', metavar='index', type=str,
                        help='index name to dump')

    args = parser.parse_args()
    index_name = args.index

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

    indices = es.cat.indices(v=True).strip().split("\n")
    index_list = [index.split()[2] for index in indices]

    if not os.path.exists(OTHER_JSON_DIR_FULL_PATH):
        os.mkdir(OTHER_JSON_DIR_FULL_PATH)

    for index in index_list:
        if index_name in index:
            process_index(index_name)
