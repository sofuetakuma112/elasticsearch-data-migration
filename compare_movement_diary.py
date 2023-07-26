import json
from tqdm import tqdm


MOVEMENT_DIARY_JSON_PATH = "/media/sofue/C45ACC905ACC8122/old_elasticsearch_data/other/jsons/movement_diary.json"
MOVEMENT_DIARY_01_JSON_PATH = "/media/sofue/C45ACC905ACC8122/old_elasticsearch_data/other/jsons/movement_diary01.json"


# 特定のフィールドの値が一致するオブジェクトを検索する関数を定義する
def find_objects_by_field(objects, field, value):
    for i, obj in enumerate(objects):
        if obj.get(field) == value:
            return i

    return -1


with open(MOVEMENT_DIARY_JSON_PATH, "r") as f1:
    with open(MOVEMENT_DIARY_01_JSON_PATH, "r") as f2:
        not_found_data1 = []

        data1 = json.load(f1)
        data2 = json.load(f2)
        data2_sources = list(map(lambda doc: doc["_source"], data2))
        for doc in tqdm(data1, desc="document"):
            dt_S = doc["_source"]["dt_S"]

            if dt_S is None:
                inspection = doc["_source"]["inspection"]
                if inspection is None:
                    not_found_data1.append(doc["_source"])
                    continue

                print(f"dt_S => {dt_S}, inspection => {inspection}")
                index = find_objects_by_field(data2_sources, "inspection", inspection)
            else:
                index = find_objects_by_field(data2_sources, "dt_S", dt_S)

            if index == -1:
                print(json.dumps(doc["_source"], indent=4))
                raise Exception("No matching objects found.")
            else:
                del data2_sources[index]

        # for s1, s2 in zip(not_found_data1, data2_sources):
        #     mismatched_fields = []
        #     for key, value in s1.items():
        #         if s2.get(key) != value:
        #             mismatched_fields.append((key, [value, s2.get(key)]))
        #     for field in mismatched_fields:
        #         print(f"{field[0]} => s1: {field[1][0]}, s2: {field[1][1]}")
