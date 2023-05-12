import json
import os


def read_json_files(jsons_dir, exclude_files=[]):
    """
    指定されたディレクトリにあるJSONファイルを読み込んで、すべてのデータを格納するリストを返す関数。

    :param jsons_dir: JSONファイルが格納されているディレクトリのパス
    :return: JSONファイルから読み込んだすべてのデータが格納されたリスト
    """
    # JSONファイル名をリストで取得する
    json_filenames = [f for f in os.listdir(jsons_dir) if f.endswith(".json")]

    # 各JSONファイルを読み込んで、データを格納するためのリスト
    all_json_data = []

    # 各JSONファイルを読み込んで、all_json_dataに格納する
    for filename_with_ext in json_filenames:
        if filename_with_ext in exclude_files:
            continue

        filepath = os.path.join(jsons_dir, filename_with_ext)  # ファイルパスを作成する
        try:
            with open(filepath, "r") as f:
                data = json.load(f)
                all_json_data.extend(data)
        except FileNotFoundError as e:
            print(f"Error on {filename_with_ext}: {e}")
            raise e
        except json.JSONDecodeError as e:
            print(f"Error on {filename_with_ext}: {e}")
            raise e

    return all_json_data


def dict_equal(dict1, dict2):
    """
    2つの辞書が完全に一致しているかどうかを調べる関数
    """
    # キーのリストをアルファベット順にソート
    keys1 = sorted(dict1.keys())
    keys2 = sorted(dict2.keys())

    # ソートしたキーのリストを使用して、各項目が一致するかどうかを調べる
    if keys1 != keys2:
        return False

    for key in keys1:
        if dict1[key] != dict2[key]:
            return False

    return True
