import os
import json
from tqdm import tqdm
import datetime

from constants import JSON_DIR_PATH, OUT_JSON_DIR_PATH
from utils.datetime import parse_utctime
from utils.json import process_json_lines


def count_matching_files(directory):
    utctime_set = set()
    duplicate_count = 0
    oldest_utctime = None
    newest_utctime = None

    filtered_docs = []

    json_files = [file for file in os.listdir(directory) if file.endswith(".json")]

    for file in tqdm(json_files, desc="Processing"):
        file_path = os.path.join(directory, file)
        with open(file_path, "r") as inp_f:
            try:
                docs = json.load(inp_f)
            except json.JSONDecodeError:
                docs = process_json_lines(file_path)
            for doc in docs:
                if "_source" in doc and "utctime" in doc["_source"]:
                    utctime = doc["_source"]["utctime"]
                    if utctime in utctime_set:
                        duplicate_count += 1
                    else:
                        utctime_set.add(utctime)
                        filtered_docs.append(doc)

                        parsed_utctime = parse_utctime(utctime)
                        if oldest_utctime is None or parsed_utctime < oldest_utctime:
                            oldest_utctime = parsed_utctime
                        if newest_utctime is None or parsed_utctime > newest_utctime:
                            newest_utctime = parsed_utctime
                else:
                    raise Exception("不正なドキュメントデータ")

    new_file_path = os.path.join(
        OUT_JSON_DIR_PATH,
        f"filtered_{datetime.datetime.now().strftime('%Y%m%d%H%M%S')}.json",
    )

    with open(new_file_path, "w") as f:
        json.dump(filtered_docs, f)            
    
    return duplicate_count, oldest_utctime, newest_utctime


if __name__ == "__main__":
    duplicate_count, oldest_utctime, newest_utctime = count_matching_files(
        JSON_DIR_PATH
    )
    print("Duplicate utctime count:", duplicate_count)
    print("Oldest utctime:", oldest_utctime)
    print("Newest utctime:", newest_utctime)
