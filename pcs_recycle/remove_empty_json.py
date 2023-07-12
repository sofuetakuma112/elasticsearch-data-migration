import os
import json
from tqdm import tqdm

JSON_DIR_PATH = "/media/sofue/C45ACC905ACC8122/pcs_recycle/local"


def find_empty_json_files(directory):
    empty_files = []
    json_files = [file for file in os.listdir(directory) if file.endswith(".json")]

    for file in tqdm(json_files, desc="Processing"):
        file_path = os.path.join(directory, file)
        with open(file_path, "r") as f:
            data = json.load(f)
            if isinstance(data, list) and len(data) == 0:
                empty_files.append([file_path, data])
                os.remove(file_path)  # ファイルを削除する

    return empty_files


empty_json_files = find_empty_json_files(JSON_DIR_PATH)
for file in empty_json_files:
    print(file)
