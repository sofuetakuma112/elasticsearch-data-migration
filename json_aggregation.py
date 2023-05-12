import argparse
import json
import glob

from constants import JSON_DIR_FULL_PATH


def process_json_files(exclude_files):
    # jsonsディレクトリ内のJSONファイルを読み込む
    for file_path in glob.glob(f"{JSON_DIR_FULL_PATH}/*.json"):
        file_name = file_path.split("/")[-1]

        # 除外ファイルでない場合のみ処理を行う
        if file_name not in exclude_files:
            # ユニークな値のセットを作成
            unique_values = set()

            with open(file_path, "r") as f:
                json_data = json.load(f)

            # _source.numberキーの値を集計
            for obj in json_data:
                if "_source" in obj and "number" in obj["_source"]:
                    unique_values.add(obj["_source"]["number"])

            # ユニークな値のセットを出力
            print(f"{file_name} => {list(unique_values)}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Process JSON files and output unique values."
    )
    parser.add_argument("--exclude-files", nargs="*", help="JSON file names to exclude")
    args = parser.parse_args()

    exclude_files = set(args.exclude_files) if args.exclude_files else set()
    process_json_files(exclude_files)
