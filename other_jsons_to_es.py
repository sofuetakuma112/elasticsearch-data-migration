import argparse
import json
import os
from elasticsearch import Elasticsearch
from dotenv import load_dotenv
from tqdm import tqdm

from utils.constants import OTHER_JSON_DIR_FULL_PATH


def insert_data(index):
    with open(f"{OTHER_JSON_DIR_FULL_PATH}/{index}.json", "r") as f:
        data = json.load(f)

    with tqdm(total=len(data)) as pbar:
        for document in data:
            source = document["_source"]
            try:
                es.index(index=index, body=source)
                pbar.update(1)
            except Exception as e:
                print(f"Failed to insert source: {e}")

    print(f"{index}のデータインサート終了")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Insert Elasticsearch data from JSON files."
    )
    parser.add_argument("index", metavar="index", type=str, help="index name")

    args = parser.parse_args()
    index_name = args.index

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

    insert_data(index_name)
