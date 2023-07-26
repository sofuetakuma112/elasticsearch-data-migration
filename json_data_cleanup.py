import argparse
import os
import sqlite3
from typing import List, Dict
import numpy as np
from tqdm import tqdm
from utils.constants import (
    JSON_DIR_FULL_PATH,
    OLD_ELASTICSEARCH_DATA_FULL_PATH,
    SQLITE_DIR_FULL_PATH,
)
from datetime import datetime, timezone, timedelta
from utils.date import parse_datetime
from utils.lib import read_json_file_line_by_line, get_json_file_line_count
from utils.type_convert import convert_to_float


def convert_utc_to_jst(utctime_dt: datetime) -> datetime:
    jst_offset = timedelta(hours=9)  # UTC+9:00のオフセット
    return utctime_dt.replace(tzinfo=timezone.utc).astimezone(timezone(jst_offset))


def initialize_database(database_file_path):
    conn = sqlite3.connect(database_file_path)
    cursor = conn.cursor()

    create_table_query = """
    CREATE TABLE IF NOT EXISTS co2 (
        number TEXT NOT NULL,
        JPtime TEXT NOT NULL,
        TEMP REAL,
        utctime TEXT,
        RH REAL,
        ip TEXT,
        PPM REAL,
        Temperature REAL,
        data TEXT,
        index_name TEXT,
        ms TEXT,
        PRIMARY KEY (number, JPtime)
    )
    """
    cursor.execute(create_table_query)

    # 変更をコミットして接続を閉じます
    conn.commit()

    return conn, cursor


# 読み込んだJSONデータを引数に実行する関数
# JSON単位で実行される
def remove_duplicate_data(
    conn, cursor, directory, target_json_files=None
) -> List[Dict]:
    if target_json_files is None:
        target_json_files = os.listdir(directory)

    for file_name in tqdm(os.listdir(directory), desc="Processing Files", unit="file"):
        if not file_name.endswith(".json"):
            continue

        if not file_name in target_json_files:
            continue

        file_path = os.path.join(directory, file_name)

        file_lines = read_json_file_line_by_line(file_path)
        total_lines = get_json_file_line_count(file_path)

        for doc in tqdm(
            file_lines,
            total=total_lines,
            desc=file_name,
            unit="line",
        ):
            source = doc["_source"]

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

            if utctime:
                # utctimeをjstに変換して代用する
                utctime_dt = parse_datetime(utctime, "utc")
                jptime_dt = convert_utc_to_jst(utctime_dt)
            if jptime:
                # jptimeがある場合は、jptimeを優先的に使う
                jptime_dt = parse_datetime(jptime, "jst")

            jptime_str = jptime_dt.strftime("%Y-%m-%dT%H:%M:%S.%f")

            utctime_str = None
            if utctime_dt:
                utctime_str = utctime_dt.strftime("%Y-%m-%dT%H:%M:%S.%f")

            temp_float = convert_to_float(temp)
            Temperature_float = convert_to_float(temperature)

            temp_value = temp_float or Temperature_float

            try:
                if temp_value is not None and np.isnan(temp_value):
                    print(f"temp_valueがNaN => temp: {temp}, temperature: {temperature}")
            except TypeError:
                print(f"temp_value: {temp_value}")
                np.isnan(temp_value)

            type_converted_data = {
                "number": room_number,
                "JPtime": jptime_str,
                "TEMP": temp_value,
                "utctime": utctime_str,
                "RH": convert_to_float(rh),
                "ip": source.get("ip"),
                "PPM": convert_to_float(ppm),
                "Temperature": Temperature_float,
                "data": source.get("data"),
                "index_name": source.get("index_name"),
                "ms": source.get("ms"),
            }

            upsert_query = """
            INSERT INTO co2 (number, JPtime, TEMP, utctime, RH, ip, PPM, Temperature, data, index_name, ms) 
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(number, JPtime)
            DO UPDATE SET 
                TEMP = IFNULL(TEMP, excluded.TEMP),
                utctime = IFNULL(utctime, excluded.utctime),
                RH = IFNULL(RH, excluded.RH),
                ip = IFNULL(ip, excluded.ip),
                PPM = IFNULL(PPM, excluded.PPM),
                Temperature = IFNULL(Temperature, excluded.Temperature),
                data = IFNULL(data, excluded.data),
                index_name = IFNULL(index_name, excluded.index_name),
                ms = IFNULL(ms, excluded.ms)
            """
            cursor.execute(upsert_query, tuple(type_converted_data.values()))

        # ループが終わったら変更をコミット
        conn.commit()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Modify the paths for the SQLite and JSON directory"
    )
    parser.add_argument(
        "--sqlite_dir_name",
        type=str,
        required=True,
        help="SQLite database directory name",
    )
    parser.add_argument(
        "--json_dir_name", type=str, required=True, help="JSON directory name"
    )

    args = parser.parse_args()

    sqlite_dir_name = args.sqlite_dir_name
    json_dir_name = args.json_dir_name

    conn, cursor = initialize_database(
        f"{OLD_ELASTICSEARCH_DATA_FULL_PATH}/{sqlite_dir_name}/co2.db"
    )

    removed_data = remove_duplicate_data(
        conn, cursor, f"{OLD_ELASTICSEARCH_DATA_FULL_PATH}/{json_dir_name}"
    )

    # 接続を閉じる
    conn.close()
