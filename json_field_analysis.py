import argparse
import json
import os
from collections import defaultdict
import ijson

from utils.constants import JSON_DIR_FULL_PATH


def analyze_json_files(directory, target_json_files=None):
    field_types = defaultdict(set)

    print(f"target_json_files: {target_json_files}")

    # ディレクトリ配下のJSONファイルをジェネレーターでオブジェクトごとに読み込む
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
                        for field, value in source.items():
                            current_type = type(value).__name__
                            if current_type not in field_types[field]:
                                field_types[field].add(current_type)

                                if field in ["PPM", "Temperature", "RH"] and isinstance(
                                    value, str
                                ):
                                    print(f"{field} => {value}")

                                if (
                                    field in ["RH", "PPM", "TEMP"]
                                    and type(value).__name__ == "NoneType"
                                ):
                                    # json上でnullのフィールド
                                    print(f"{field} => {value}")

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
    # コマンドライン引数のパース
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--json-file",
        nargs="+",
        help="Specify one or more JSON files to analyze",
    )
    args = parser.parse_args()

    json_files = os.listdir(JSON_DIR_FULL_PATH)
    EXCLUDE_FILES = [
        "co2_el43.json",
        "co2_e411.json",
        "co2_el24.json",
        "co2_el35.json",
        "co2_el22.json",
        "co2_el23.json",
        "co2_el26.json",
        "co2_el32.json",
        "co2_e591.json",
        "co2_el13.json",
        "co2_el44.json",
        "co2_el14.json",
    ]

    filtered_files = [file for file in json_files if file not in EXCLUDE_FILES]

    # JSONファイルのフィールドを集計して出力
    if args.json_file is None:
        analyze_json_files(JSON_DIR_FULL_PATH, target_json_files=filtered_files)
    else:
        analyze_json_files(JSON_DIR_FULL_PATH, target_json_files=args.json_file)
