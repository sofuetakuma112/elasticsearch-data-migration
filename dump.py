import json
import os
import psutil
from elasticsearch import Elasticsearch
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor


def file_exists(dir_path, file_name):
    for f in os.listdir(dir_path):
        if f == file_name:
            return True
    return False


def process_index(index):
    json_file_names = list(
        map(
            lambda file_name_with_ext: os.path.splitext(file_name_with_ext)[0],
            os.listdir("jsons"),
        )
    )
    if "co2" in index:
        if index in json_file_names:
            # print(f"skipped index: {index}")
            return
        # if file_exists("jsons", f"{index}.json"):
        #     return
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
        s_size = data["hits"]["total"]["value"]

        # Save the documents in a JSON file with the index name
        with open(f"jsons/{index}.json", "w") as f:
            while s_size > 0:
                documents = data["hits"]["hits"]
                json.dump(documents, f, ensure_ascii=False, indent=4)
                f.write("\n")

                data = es.scroll(scroll_id=s_id, scroll=s_time, request_timeout=150)
                s_id = data["_scroll_id"]
                s_size = len(data["hits"]["hits"])


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

# Create the "jsons" directory if it doesn't exist
if not os.path.exists("jsons"):
    os.mkdir("jsons")

# Determine the maximum number of threads based on the CPU core count
# max_threads = psutil.cpu_count(logical=True)

# # Process the indices in parallel using a ThreadPoolExecutor
# with ThreadPoolExecutor(max_workers=max_threads) as executor:
#     executor.map(process_index, index_list)

for index in index_list:
    process_index(index)
