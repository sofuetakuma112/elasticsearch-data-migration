from utils.lib import read_json_file_line_by_line, get_json_file_line_count
import os
from tqdm import tqdm

# ダンプされたJSONのファイルが保存されているディレクトリ
DIRECTORY_PATH = "/media/sofue/C45ACC905ACC8122/133.71.106.141_elasticsearch_data/co2/jsons_missing_jptime_docs"

# すべてのJSONファイルを読み込み
all_utctime_values = []

for filename in os.listdir(DIRECTORY_PATH):
    if filename.endswith(".json"):
        file_path = os.path.join(DIRECTORY_PATH, filename)
        file_lines = read_json_file_line_by_line(file_path)
        total_lines = get_json_file_line_count(file_path)

        for entry in tqdm(
            file_lines,
            total=total_lines,
            desc=filename,
            unit="line",
        ):
            # エントリが '_source' キーを持つ場合のみ処理を進める
            if "_source" in entry:
                # 'JPtime' キーが存在する場合、例外をスロー
                if "JPtime" in entry["_source"]:
                    raise ValueError(
                        f"Unexpected 'JPtime' field found in entry: {entry}"
                    )

                # 'utctime' キーが存在する場合、値を取得
                if "utctime" in entry["_source"]:
                    all_utctime_values.append(entry["_source"]["utctime"])
                else:
                    raise ValueError(f"Missing 'utctime' field in entry: {entry}")
            else:
                raise ValueError(f"Missing '_source' field in entry: {entry}")

# すべてのutctime値をソート
all_utctime_values.sort()

# 最も古いutctimeの値を出力
if all_utctime_values:
    print("The oldest utctime is:", all_utctime_values[0])
else:
    print("No utctime values found.")
