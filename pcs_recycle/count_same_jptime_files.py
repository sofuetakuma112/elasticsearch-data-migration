import os
import json
import sys
from tqdm import tqdm


def count_matching_files(directory, specific_file=None):
    jptime_set = set()
    duplicate_count = 0

    if specific_file is None:
        json_files = [file for file in os.listdir(directory) if file.endswith(".json")]
    else:
        json_files = [specific_file]

    print(f"json_files: {json_files}")

    for file in tqdm(json_files, desc="json file"):
        file_path = os.path.join(directory, file)
        with open(file_path, "r") as f:
            try:
                data = json.load(f)
                for doc in tqdm(data, desc="document"):
                    if "_source" in doc and "JPtime" in doc["_source"]:
                        jptime = doc["_source"]["JPtime"]
                        if jptime in jptime_set:
                            duplicate_count += 1
                        else:
                            jptime_set.add(jptime)
            except json.JSONDecodeError:
                pass

    return duplicate_count


if len(sys.argv) > 1:
    JSON_FILE_PATH = sys.argv[1]
    JSON_DIR_PATH, SPECIFIC_FILE = os.path.split(JSON_FILE_PATH)
else:
    JSON_DIR_PATH = "/media/sofue/C45ACC905ACC8122/pcs_recycle/jsons"
    SPECIFIC_FILE = None

duplicate_count = count_matching_files(JSON_DIR_PATH, SPECIFIC_FILE)
print("Duplicate JPtime count:", duplicate_count)
