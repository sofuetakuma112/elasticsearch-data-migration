from tqdm import tqdm
from utils.constants import JSON_DIR_FULL_PATH
from utils.lib import read_json_file_line_by_line, get_json_file_line_count
import os
import logging


def setup_logger(log_file):
    # ロギングの設定
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    # ログファイルへのハンドラを作成
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(logging.INFO)

    # フォーマッタの設定
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)

    # ロガーにハンドラを追加
    logger.addHandler(file_handler)

    return logger


if __name__ == "__main__":
    # ログファイルのパス
    log_file = "out/log.txt"

    # ロガーのセットアップ
    logger = setup_logger(log_file)

    # これまでの最小と最大のキーの数を初期化
    min_key_count = float("inf")
    max_key_count = float("-inf")

    for file_name in tqdm(
        os.listdir(JSON_DIR_FULL_PATH), desc="Processing Files", unit="file"
    ):
        file_path = os.path.join(JSON_DIR_FULL_PATH, file_name)

        file_lines = read_json_file_line_by_line(file_path)
        total_lines = get_json_file_line_count(file_path)

        for doc in tqdm(
            file_lines,
            total=total_lines,
            desc=file_name,
            unit="line",
        ):
            source = doc["_source"]
            id = doc["_id"]

            # sourceのキーの数がこれまでの最小より少ない、もしくは最大より多い場合、_idとキー数をメモする
            # sourceのキーの数を取得
            key_count = len(source.keys())

            # キーの数がこれまでの最小より少ない場合、最小値を更新しidとキー数をメモする
            if key_count < min_key_count:
                min_key_count = key_count
                min_key_info = (file_name, id, key_count)

            # キーの数がこれまでの最大より多い場合、最大値を更新しidとキー数をメモする
            if key_count > max_key_count:
                max_key_count = key_count
                max_key_info = (file_name, id, key_count)

        logger.info(f"min_key_info: {min_key_info}")
        logger.info(f"max_key_info: {max_key_info}")

    # 最小値と最大値を出力
    print("Minimum Key Count:", min_key_count)
    print("Minimum Key Info:", min_key_info)
    print("Maximum Key Count:", max_key_count)
    print("Maximum Key Info:", max_key_info)
