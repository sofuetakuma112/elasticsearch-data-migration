import json
import os
from typing import List, Dict
from elasticsearch import Elasticsearch
from lib import read_json_files
from datetime import datetime, timezone, timedelta


def parse_jptime(jptime: str) -> datetime:
    try:
        jptime_dt = datetime.strptime(jptime, "%Y-%m-%dT%H:%M:%S.%f")
    except ValueError:
        jptime_dt = datetime.strptime(jptime, "%Y-%m-%dT%H:%M:%S")
    return jptime_dt


def parse_utctime(utctime: str) -> datetime:
    try:
        utctime_dt = datetime.strptime(utctime, "%Y-%m-%dT%H:%M:%S%z")
    except ValueError:
        utctime_dt = datetime.strptime(utctime, "%Y-%m-%dT%H:%M:%S.%f")
    return utctime_dt


def convert_utc_to_jst(utctime_dt: datetime) -> datetime:
    jst_offset = timedelta(hours=9)  # UTC+9:00のオフセット
    return utctime_dt.replace(tzinfo=timezone.utc).astimezone(timezone(jst_offset))


def remove_duplicate_data(data: List[Dict]) -> List[Dict]:
    """
    Remove duplicate entries from the data and add only unique data to a new list.

    :param data: original data
    :return: list of unique data without duplicates
    """
    unique_set = set()
    unique_data_dict = {}

    for d in data:
        jptime = d["_source"].get("JPtime")
        utctime = d["_source"].get("utctime")

        if not (jptime or utctime):
            continue

        if jptime:
            dt = parse_jptime(jptime)
        else:
            utctime_dt = parse_utctime(utctime)
            dt = convert_utc_to_jst(utctime_dt)

        str_time = dt.strftime("%Y-%m-%dT%H:%M:%S.%f")
        key = (d["_source"]["number"].upper(), str_time)

        if key not in unique_set:
            unique_set.add(key)
            unique_data_dict[key] = d
        else:
            original_data = unique_data_dict[key]
            original_field_count = len(original_data["_source"])
            duplicate_field_count = len(d["_source"])

            print(f"original_data: {original_data['_source']}")
            print(f"duplicate_data: {d['_source']}")

            if duplicate_field_count > original_field_count:
                unique_data_dict[key] = d
                print("重複データがオリジナルのデータと入れ替えられました")

    return list(unique_data_dict.values())


def classify_json_data_by_year(json_data: List[Dict]) -> Dict[str, List[Dict]]:
    """
    JSONデータを_source.JPtimeが2023より以前のデータと2023年のデータに分類する関数。

    :param json_data: JSONデータのリスト
    :return: 2023年以前のデータと2023年のデータに分類された辞書オブジェクト
    """
    data_before_2023 = []
    data_in_2023 = []
    for d in json_data:
        jp_time_str = d["_source"]["JPtime"]
        jp_time = datetime.strptime(jp_time_str, "%Y-%m-%dT%H:%M:%S.%f")
        if jp_time.year < 2023:
            data_before_2023.append(d)
        else:
            data_in_2023.append(d)
    return {"before_2023": data_before_2023, "in_2023": data_in_2023}


def save_json_data_to_index(data: List[Dict], index_name: str):
    """
    JSONデータを指定したindex名のインデックスに保存する関数。

    :param data: JSONデータのリスト
    :param index_name: 保存するindex名
    """
    # Elasticsearchの接続情報を設定する
    es = Elasticsearch(
        [
            f"http://{os.getenv('TARGET_ELASTICSEARCH_HOST')}:{os.getenv('TARGET_ELASTICSEARCH_PORT')}"
        ],
        http_auth=(
            os.getenv("TARGET_ELASTICSEARCH_USERNAME"),
            os.getenv("TARGET_ELASTICSEARCH_PASSWORD"),
        ),
    )

    # ElasticsearchにJSONデータを保存する
    for d in data:
        body = json.dumps(d["_source"])
        query = {
            "query": {
                "bool": {
                    "must": [
                        {"match": {"_source.number": d["_source"]["number"]}},
                        {"match": {"_source.JPtime": d["_source"]["JPtime"]}},
                    ]
                }
            }
        }
        search_res = es.search(index=index_name, body=query)
        if search_res["hits"]["total"]["value"] == 0:
            # 同じnumberとJPTimeの組み合わせは存在しない
            res = es.index(index_name, body=body)
            print(res)
        else:
            # 同じnumberとJPTimeの組み合わせが既に存在するので保存しない
            print(
                f"Data for {d['_source']['number']} with JPtime {d['_source']['JPtime']} already exists in the index."
            )


if __name__ == "__main__":
    all_json_data = read_json_files("jsons", ["co2_el35.json"])

    removed_data = remove_duplicate_data(all_json_data)
    print(
        f"len(all_json_data) => len(removed_data): {len(all_json_data)} => {len(removed_data)}"
    )

    # classified = classify_json_data_by_year(removed_data)
    # before_2023_data = classified["before_2023"]
    # in_2023_data = classified["in_2023"]

    # save_json_data_to_index(before_2023_data, "2022_co2")
    # save_json_data_to_index(in_2023_data, "2023_co2")
