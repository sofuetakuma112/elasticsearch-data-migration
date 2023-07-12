import os
import json
from tqdm import tqdm


def count_matching_files(directory):
    missing_source_count = 0

    json_files = [file for file in os.listdir(directory) if file.endswith(".json")]

    for file in tqdm(json_files, desc="Processing"):
        file_path = os.path.join(directory, file)
        with open(file_path, "r") as f:
            try:
                data = json.load(f)
                for doc in data:
                    if "_source" not in doc:
                        missing_source_count += 1
                        # print(json.dumps(doc, indent=4))
            except json.JSONDecodeError:
                pass

    return missing_source_count


JSON_DIR_PATH = "/media/sofue/C45ACC905ACC8122/pcs_recycle/jsons"
missing_source_count = count_matching_files(JSON_DIR_PATH)
print("Missing _source count:", missing_source_count)
