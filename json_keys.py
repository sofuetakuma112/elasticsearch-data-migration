import collections.abc
from utils.constants import JSON_DIR_FULL_PATH
from utils.lib import read_json_files
import os
import json
from typing import List, Set


def flatten(obj):
    def _flatten(obj, parent_key="", sep="."):
        items = []
        for k, v in obj.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, collections.abc.MutableMapping):
                items.extend(_flatten(v, new_key, sep=sep).items())
            else:
                items.append((new_key, v))
        return dict(items)

    return _flatten(obj)


def get_json_files_keys(jsons_dir: str) -> List[Set[str]]:
    """
    JSONディレクトリ下の各JSONファイルを読み込んで、
    flatten関数で平坦化したDictのkeyのユニークな集合をJSONファイルごとに計算して、
    JSONファイルの集合全体でのflatten関数で平坦化したDictのkeyのユニークな集合と比較して、
    各JSONファイルごとに存在するキーと存在しないキーを表示する関数。

    :param jsons_dir: JSONファイルが格納されているディレクトリのパス
    :return: 各JSONファイルに含まれるキーの集合のリスト
    """
    # JSONファイル名をリストで取得する
    json_filenames = [f for f in os.listdir(jsons_dir) if f.endswith(".json")]

    # 各JSONファイルに含まれるキーの集合のリスト
    all_keys = set()
    file_keys_list = []

    # 各JSONファイルを読み込んで、flatten関数で平坦化したDictのkeyのユニークな集合を計算する
    file_names_with_diff_keys = set()
    for filename in json_filenames:
        filepath = os.path.join(jsons_dir, filename)  # ファイルパスを作成する
        keys_per_file = set()
        print(f"current JSON file: {filename}")
        try:
            with open(filepath, "r") as f:
                data = json.load(f)
                for doc in data:
                    flattened_data = flatten(doc["_source"])
                    file_keys = set(flattened_data.keys())

                    if len(keys_per_file) != 0 and file_keys != keys_per_file:
                        # 同じJSON内でキーのセットがドキュメントごとに異なる
                        if not filename in file_names_with_diff_keys:
                            print(f"キーのセットがドキュメント間で異なるJSONファイル: {filename}")
                            print(f"該当のドキュメントID: {doc['_id']}")
                            file_names_with_diff_keys.add(filename)

                    keys_per_file.update(file_keys)
                    file_keys_list.append(file_keys)
                    all_keys.update(file_keys)
        except FileNotFoundError as e:
            print(f"Error on {filename}: {e}")
            raise e
        except json.JSONDecodeError as e:
            print(f"Error on {filename}: {e}")
            raise e
        

    # ユニークなキーを表示する
    print("Unique Keys:")
    for key in all_keys:
        print(key)

    for i, filename in enumerate(json_filenames):
        file_keys = file_keys_list[i]
        present_keys = all_keys.intersection(file_keys)
        missing_keys = all_keys.difference(file_keys)

        # Present keysを集合から辞書に変換する
        present_dict = {key: "○" for key in present_keys}
        missing_dict = {key: "☓" for key in missing_keys}

        # テーブルのヘッダを作成する
        header = "| Filename | "
        for key in all_keys:
            header += f"{key} | "
        print(header)

        body = f"| {filename} |"
        for key in all_keys:
            if key in present_dict:
                body += f" {present_dict[key]} |"
            else:
                body += f" {missing_dict[key]} |"
        print(body)

        print()

    return file_keys_list


if __name__ == "__main__":
    # get_json_files_keys(JSON_DIR_FULL_PATH)

    all_json_data = read_json_files(JSON_DIR_FULL_PATH, ["co2_el35.json"])

    numbers = set()
    for d in all_json_data:
        numbers.add(d["_source"]["number"])
    
    print(numbers)
