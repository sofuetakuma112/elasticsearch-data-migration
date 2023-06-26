import argparse
import os
from collections import defaultdict
from tqdm import tqdm

from utils.constants import JSON_DIR_FULL_PATH
from utils.lib import get_json_file_line_count, read_json_file_line_by_line


def analyze_json_files(directory):
    field_types = defaultdict(set)

    # ディレクトリ配下のJSONファイルをジェネレーターでオブジェクトごとに読み込む
    for file_name in os.listdir(directory):
        if file_name.endswith(".json"):
            if file_name in os.listdir(directory):
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

                    for field, value in source.items():
                        current_type = type(value).__name__
                        if current_type not in field_types[field]:
                            field_types[field].add(current_type)

                            # if field in ["PPM", "Temperature", "RH"] and isinstance(
                            #     value, str
                            # ):
                            #     print(f"{field} => {value}")

                            # if (
                            #     field in ["RH", "PPM", "TEMP"]
                            #     and type(value).__name__ == "NoneType"
                            # ):
                            #     # json上でnullのフィールド
                            #     print(f"{field} => {value}")

                print(f"field_types: {field_types}")

    # 最終的な集計結果を表示（ユニオン型を考慮）
    final_field_types = defaultdict(set)
    for field, types in field_types.items():
        type_union = " | ".join(types) if len(types) > 1 else next(iter(types))
        final_field_types[field].add(type_union)

    print("Final Field Types:")
    for field, types in final_field_types.items():
        type_names = " | ".join(types)
        print(f"{field}: {type_names}")


if __name__ == "__main__":
    analyze_json_files(JSON_DIR_FULL_PATH)
