import json
import subprocess
import os

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
    cache_file = os.path.join(
        cache_dir, f"{file_name_without_extension}.count"
    )  # キャッシュファイルのパス

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
