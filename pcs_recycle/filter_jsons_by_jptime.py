import os
import json
from tqdm import tqdm


def count_matching_files(directory):
    jptime_set = set()
    duplicate_count = 0
    new_docs = []

    json_files = [file for file in os.listdir(directory) if file.endswith(".json")]

    for file in tqdm(json_files, desc="Processing"):
        file_path = os.path.join(directory, file)
        with open(file_path, "r") as f:
            try:
                docs = json.load(f)
                for doc in docs:
                    if "_source" in doc and "JPtime" in doc["_source"]:
                        jptime = doc["_source"]["JPtime"]
                        if jptime in jptime_set:
                            duplicate_count += 1
                        else:
                            jptime_set.add(jptime)
                            new_docs.append(doc)
            except json.JSONDecodeError:
                pass

    # Write new data to a JSON file
    new_file_path = os.path.join(OUT_JSON_DIR_PATH, "filtered.json")
    with open(new_file_path, "w") as f:
        json.dump(new_docs, f)

    return duplicate_count


JSON_DIR_PATH = "/media/sofue/C45ACC905ACC8122/pcs_recycle/jsons"
OUT_JSON_DIR_PATH = "/media/sofue/C45ACC905ACC8122/pcs_recycle"
duplicate_count = count_matching_files(JSON_DIR_PATH)
print("Duplicate JPtime count:", duplicate_count)
