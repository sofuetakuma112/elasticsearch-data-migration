import json
import os
import subprocess


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


def read_json_file_line_by_line(file_path):
    with open(file_path, "r") as file:
        for line in file:
            # 1行ずつJSONとして読み込む
            json_data = json.loads(line)

            # JSONデータを返す
            yield json_data


def get_json_file_line_count(file_path):
    # 拡張子を取り除く
    file_name_without_extension = os.path.splitext(os.path.basename(file_path))[0]

    cache_dir = "cache"  # キャッシュディレクトリのパス
    cache_file = os.path.join(cache_dir, f"{file_name_without_extension}.count")  # キャッシュファイルのパス

    if os.path.exists(cache_file):  # キャッシュファイルが存在する場合
        with open(cache_file, "r") as f:
            line_count = int(f.read())
    else:
        line_count = int(
            subprocess.check_output(["wc", "-l", file_path]).decode().split(" ")[0]
        )
        # キャッシュファイルに結果を保存
        os.makedirs(cache_dir, exist_ok=True)  # 存在しない場合にのみディレクトリを作成
        with open(cache_file, "w") as f:
            f.write(str(line_count))

    return line_count
