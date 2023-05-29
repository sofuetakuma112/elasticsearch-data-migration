import os
import sqlite3
import numpy as np
from utils.constants import SQLITE_DIR_FULL_PATH
from elasticsearch import AsyncElasticsearch
from elasticsearch.helpers import async_streaming_bulk, BulkIndexError
import pandas as pd
import sqlite3
from tqdm import tqdm
from dotenv import load_dotenv
import asyncio
import logging

from utils.date import parse_datetime

# ロギングの設定
logging.basicConfig(filename="out/error.log", level=logging.ERROR)


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

        index_name = "2022_co2" if jptime_parsed.year < 2023 else "2023_co2"
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

    # SQLiteデータベースに接続
    conn = sqlite3.connect(f"{SQLITE_DIR_FULL_PATH}/co2.db")
    cursor = conn.cursor()

    # co2テーブルのデータを取得
    cursor.execute("SELECT * FROM co2")
    data = cursor.fetchall()

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

    # helpers.bulkメソッドでバルクインサートする
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
    # 例外処理
    except BulkIndexError as bulk_error:
        # エラーはリスト形式
        logging.error(bulk_error.errors)

    # データベースとElasticsearchとの接続を閉じる
    conn.close()
    await es.close()


if __name__ == "__main__":
    # イベントループを取得
    loop = asyncio.get_event_loop()
    # 並列に実行して終るまで待つ
    loop.run_until_complete(main())
