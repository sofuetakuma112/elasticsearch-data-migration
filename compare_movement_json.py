import json
from utils.constants import OTHER_JSON_DIR_FULL_PATH


def compare_json_files(file1, file2):
    with open(file1) as f1, open(file2) as f2:
        json1 = json.load(f1)
        json2 = json.load(f2)

        for obj1, obj2 in zip(json1, json2):
            source1 = obj1["_source"]
            source2 = obj2["_source"]

            if source1 != source2:
                return False, obj1, obj2

        return True, None, None


# 2つのJSONファイルのパスを指定して比較する
file1 = f"{OTHER_JSON_DIR_FULL_PATH}/movement_diary.json"
file2 = f"{OTHER_JSON_DIR_FULL_PATH}/movement_diary01.json"
result, o1, o2 = compare_json_files(file1, file2)

if result:
    print("The _source fields match.")
else:
    print("The _source fields do not match.")
    print(f"obj1: {json.dumps(o1, indent=4).encode('utf-8').decode('unicode_escape')}")
    print(f"obj2: {json.dumps(o2, indent=4).encode('utf-8').decode('unicode_escape')}")
