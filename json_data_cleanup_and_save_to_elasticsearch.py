import json
import os
import sqlite3
from typing import List, Dict
from elasticsearch import Elasticsearch
from constants import JSON_DIR_FULL_PATH
from datetime import datetime, timezone, timedelta
import ijson


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


# 読み込んだJSONデータを引数に実行する関数
# JSON単位で実行される
def remove_duplicate_data(directory) -> List[Dict]:
    for file_name in os.listdir(directory):
        if file_name.endswith(".json"):
            if target_json_files is None:
                target_json_files = os.listdir(directory)

            if file_name in target_json_files:
                file_path = os.path.join(directory, file_name)

                print(f"current processing {file_name}")
                with open(file_path, "r") as f:
                    generator = ijson.items(f, "item._source")
                    for source in generator:
                        jptime = source.get("JPtime")
                        utctime = source.get("utctime")
                        room_number = source.get("number")

                        temp = source.get("TEMP")
                        rh = source.get("RH")
                        ppm = source.get("PPM")
                        temperature = source.get("Temperature")

                        if not (jptime or utctime):
                            # jptime と utctime のどちらも存在しない
                            continue

                        if room_number is None:
                            # room_numberが存在しない
                            continue

                        room_number = source["number"].upper()

                        if jptime:
                            dt = parse_jptime(jptime)
                        else:
                            utctime_dt = parse_utctime(utctime)
                            dt = convert_utc_to_jst(utctime_dt)

                        str_jp_dt = dt.strftime("%Y-%m-%dT%H:%M:%S.%f")

                        # TODO: SQLITEにINSERTする処理に変える

                        # 複合主キーで検索
                        search_query = (
                            "SELECT * FROM my_table WHERE number = ? AND JPtime = ?"
                        )
                        search_params = (room_number, str_jp_dt)
                        cursor.execute(search_query, search_params)

                        # 検索結果の取得
                        result = cursor.fetchone()

                        column_mapping = {
                            "number": "number",
                            "JPtime": "JPtime",
                            "TEMP": "TEMP",
                            "utctime": "utctime",
                            "RH": "RH",
                            "ip": "ip",
                            "PPM": "PPM",
                            "Temperature": "Temperature",
                            "data": "data",
                            "index_name": "index_name",
                            "ms": "ms",
                        }

                        # データの準備
                        type_converted_data = {
                            "number": room_number,
                            "JPtime": str_jp_dt,
                            "TEMP": temp if temp is None else float(temp),
                            "RH": rh if rh is None else float(rh),
                            "ip": source.get("ip"),
                            "PPM": ppm if ppm is None else float(ppm),
                            "Temperature": float(temperature),
                            "data": source.get("data"),
                            "index_name": source.get("index_name"),
                            "ms": source.get("ms"),
                        }

                        # 重複がない場合にのみINSERT文を実行
                        if result is None:
                            # INSERT文の実行
                            insert_query = "INSERT INTO my_table (number, JPtime, TEMP, utctime, RH, ip, PPM, Temperature, data, index_name, ms) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
                            insert_params = (
                                type_converted_data.get(column_mapping["number"]),
                                type_converted_data.get(column_mapping["JPtime"]),
                                type_converted_data.get(column_mapping["TEMP"]),
                                type_converted_data.get(column_mapping["utctime"]),
                                type_converted_data.get(column_mapping["RH"]),
                                type_converted_data.get(column_mapping["ip"]),
                                type_converted_data.get(column_mapping["PPM"]),
                                type_converted_data.get(column_mapping["Temperature"]),
                                type_converted_data.get(column_mapping["data"]),
                                type_converted_data.get(column_mapping["index_name"]),
                                type_converted_data.get(column_mapping["ms"]),
                            )
                            cursor.execute(insert_query, insert_params)
                            print("Data inserted successfully.")
                        else:
                            # 重複データが見つかった場合
                            # データベースの既存フィールドが NULL で、新データが NULL でない場合にのみ更新
                            fields = [
                                "TEMP",
                                "utctime",
                                "RH",
                                "ip",
                                "PPM",
                                "Temperature",
                                "data",
                                "index_name",
                                "ms",
                            ]
                            for field in fields:
                                if (
                                    result[field] is None
                                    and source.get(field) is not None
                                ):
                                    update_query = f"UPDATE my_table SET {field} = ? WHERE number = ? AND JPtime = ?"
                                    update_params = (
                                        type_converted_data[field],
                                        room_number,
                                        str_jp_dt,
                                    )
                                    cursor.execute(update_query, update_params)
                                    print(f"Updated field {field} for existing record.")

                        # 変更をコミット
                        conn.commit()


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
        hosts=[
            f"http://{os.getenv('TARGET_ELASTICSEARCH_HOST')}:{os.getenv('TARGET_ELASTICSEARCH_PORT')}"
        ],
        scheme="http",
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
    # SQLiteデータベースへの接続
    conn = sqlite3.connect("example.db")
    # カーソルを取得
    cursor = conn.cursor()

    removed_data = remove_duplicate_data(JSON_DIR_FULL_PATH)

    # classified = classify_json_data_by_year(removed_data)
    # before_2023_data = classified["before_2023"]
    # in_2023_data = classified["in_2023"]

    # save_json_data_to_index(before_2023_data, "2022_co2")
    # save_json_data_to_index(in_2023_data, "2023_co2")

    # 接続を閉じる
    conn.close()
