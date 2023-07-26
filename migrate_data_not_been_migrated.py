import os
import sqlite3

from dotenv import load_dotenv

from utils.constants import (
    OLD_ELASTICSEARCH_DATA_FULL_PATH,
    SQLITE_DIR_FULL_PATH_20230516,
)

from datetime import datetime

import numpy as np
from elasticsearch import AsyncElasticsearch, Elasticsearch
from elasticsearch.helpers import async_streaming_bulk, BulkIndexError
import pandas as pd
import sqlite3
from tqdm import tqdm
import asyncio
import logging

from utils.date import parse_datetime


def load_within_range_data():
    begin_utctime = None
    end_utctime = None

    # 1. sqliteテーブルで最も新しいタイムスタンプを取得する
    conn = sqlite3.connect(f"{SQLITE_DIR_FULL_PATH_20230516}/co2.db")
    cursor = conn.cursor()

    # SQL文を実行して最新のutctimeを取得
    cursor.execute("SELECT MAX(utctime) FROM co2")
    result = cursor.fetchone()

    # 結果を表示
    if result:
        print("sqliteファイルの最新のutctime:", result[0])
        begin_utctime = result[0]
    else:
        print("テーブルが空またはutctimeがNULLです")

    # データベース接続を閉じる
    conn.close()

    # 2. co2_modbusのラズパイがアップロードしたデータの最も古いタイムスタンプを取得する

    es_target = Elasticsearch(
        [
            f"http://{os.getenv('TARGET_ELASTICSEARCH_HOST')}:{os.getenv('TARGET_ELASTICSEARCH_PORT')}"
        ],
        basic_auth=(
            os.getenv("TARGET_ELASTICSEARCH_USERNAME"),
            os.getenv("TARGET_ELASTICSEARCH_PASSWORD"),
        ),
        request_timeout=60 * 60 * 24 * 7,
    )

    date_filter = datetime(2023, 6, 1, 0, 0, 0)

    body = {
        "query": {"range": {"utctime": {"gte": date_filter.isoformat()}}},
        "sort": [{"utctime": {"order": "asc"}}],
        "size": 1,
    }

    response = es_target.search(index="co2_modbus", body=body)

    end_utctime = response["hits"]["hits"][0]["_source"]["utctime"]

    print("移行後のElasticSearchのラズパイが上げた最古のutctime:", end_utctime)

    # 3. begin_utctimeより未来でかつ、end_utctimeより過去のドキュメントを取得する

    # .dbファイルのバス
    db_file_path = f"{OLD_ELASTICSEARCH_DATA_FULL_PATH}/sqlite_after_20230501/co2.db"

    conn = sqlite3.connect(db_file_path)
    cursor = conn.cursor()

    # utctimeがbegin_utctimeより未来でかつend_utctimeより過去のレコードを選択
    cursor.execute(
        f"SELECT * FROM co2 WHERE utctime > '{begin_utctime}' AND utctime < '{end_utctime}'"
    )

    result = cursor.fetchall()

    print("begin_utctimeより未来でかつ、end_utctimeより過去のドキュメントの数:", len(result))

    # # 最古のutctimeを取得
    # cursor.execute(f"SELECT MIN(utctime) FROM co2 WHERE utctime > '{begin_utctime}' AND utctime < '{end_utctime}'")
    # oldest_utctime = cursor.fetchone()
    # print("最も古いutctime:", oldest_utctime[0] if oldest_utctime else None)

    # # 最新のutctimeを取得
    # cursor.execute(f"SELECT MAX(utctime) FROM co2 WHERE utctime > '{begin_utctime}' AND utctime < '{end_utctime}'")
    # latest_utctime = cursor.fetchone()
    # print("最も新しいutctime:", latest_utctime[0] if latest_utctime else None)

    # データベース接続を閉じる
    conn.close()

    return result


# ロギングの設定
logging.basicConfig(filename="out/error.log", level=logging.ERROR)


# TemperatureがNaNになる可能性があるので、その際は弾く
def filter_non_numeric_nan(doc):
    """
    valueがNoneでなく、数値型でないか（not isinstance(value, (int, float))）、
    もしくは数値型であるがNaNではない（not np.isnan(value)）場合に限り、
    キーと値のペアが新しい辞書に含まれる

    :param doc: A dictionary where some values may be None or NaN.
    :return: A new dictionary where keys associated with None or NaN values have been removed.
    """
    return {
        key: value
        for key, value in doc.items()
        if value is not None
        and (not isinstance(value, (int, float)) or not np.isnan(value))
    }


async def generate_bulk_data(df):
    for row in tqdm(df.itertuples(index=False), total=len(df)):
        (
            number,
            jp_time,
            temp,
            utc_time,
            rh,
            ip,
            ppm,
            temperature,
            data,
            index_name,
            ms,
        ) = row

        if jp_time:
            jptime_parsed = parse_datetime(jp_time, "jst")
        if utc_time:
            utctime_parsed = parse_datetime(utc_time, "utc")

        index_name = "co2_modbus"
        doc = {
            "number": number,
            "JPtime": jptime_parsed,
            "TEMP": temp,
            "utctime": utctime_parsed,
            "RH": rh,
            "ip": ip,
            "PPM": ppm,
            "Temperature": temperature,
            "data": data,
            "index_name": index_name,
            "ms": ms,
        }
        doc = filter_non_numeric_nan(doc)
        yield {"_op_type": "create", "_index": index_name, "_source": doc}


async def main():
    """
    JSONデータを指定したindex名のインデックスに保存する関数。

    :param data: JSONデータのリスト
    :param index_name: 保存するindex名
    """

    # Elasticsearchの接続情報を設定する
    load_dotenv()
    es = AsyncElasticsearch(
        [
            f"http://{os.getenv('TARGET_ELASTICSEARCH_HOST')}:{os.getenv('TARGET_ELASTICSEARCH_PORT')}"
        ],
        basic_auth=(
            os.getenv("TARGET_ELASTICSEARCH_USERNAME"),
            os.getenv("TARGET_ELASTICSEARCH_PASSWORD"),
        ),
        request_timeout=60 * 60 * 24 * 7,
    )

    data = load_within_range_data()

    # pandasのデータフレームに変換
    df = pd.DataFrame(
        data,
        columns=[
            "number",
            "JPtime",
            "TEMP",
            "utctime",
            "RH",
            "ip",
            "PPM",
            "Temperature",
            "data",
            "index_name",
            "ms",
        ],
    )

    # Elasticsearchへのデータの追加
    bulk_data = generate_bulk_data(df)

    try:
        # ドキュメントを複数（チャンク）に分けてバルクインサート
        async for ok, result in async_streaming_bulk(
            client=es,
            actions=bulk_data,
            chunk_size=100,
        ):
            # 各チャンクごとの実行結果を取得
            action, result = result.popitem()
            # バルクインサートに失敗した場合
            if not ok:
                logging.error(f"failed to {result} document {action}")
    except BulkIndexError as bulk_error:
        # エラーはリスト形式
        logging.error(bulk_error.errors)

    await es.close()


if __name__ == "__main__":
    load_dotenv()

    # イベントループを取得
    loop = asyncio.get_event_loop()
    # 並列に実行して終るまで待つ
    loop.run_until_complete(main())
