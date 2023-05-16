import argparse
import os
import json
from constants import JSON_DIR_FULL_PATH
import ijson
from tqdm import tqdm

from lib import read_json_file_line_by_line


def get_json_file_line_count(file_path):
    with open(file_path, "r") as file:
        line_count = sum(1 for _ in file)
    return line_count


def cache_valid_json_file(filename):
    cache_dir = "cache"
    os.makedirs(cache_dir, exist_ok=True)
    cache_filename = os.path.join(cache_dir, filename)
    with open(cache_filename, "w") as cache_file:
        json.dump({"valid": True}, cache_file)


def is_valid_json_file_cached(filename):
    cache_dir = "cache"
    cache_filename = os.path.join(cache_dir, filename)
    return os.path.exists(cache_filename)


def check_json_files(json_files):
    if json_files is None:
        for filename in os.listdir(JSON_DIR_FULL_PATH):
            if filename.endswith(".json"):
                if is_valid_json_file_cached(filename):
                    continue
                check_json_file(JSON_DIR_FULL_PATH, filename)
    else:
        for filename in json_files:
            if is_valid_json_file_cached(filename):
                continue
            check_json_file(JSON_DIR_FULL_PATH, filename)


def check_json_file(directory, filename):
    file_path = os.path.join(directory, filename)
    try:
        total_lines = get_json_file_line_count(file_path)
        progress_bar = tqdm(total=total_lines, desc=filename, unit="line")
        for obj in read_json_file_line_by_line(file_path):
            _ = obj
            progress_bar.update(1)
        progress_bar.close()
        print(f"{filename}: Valid JSON")
        cache_valid_json_file(filename)
    except ValueError as e:
        print(f"{filename}: Invalid JSON - {str(e)}")
    except IOError as e:
        print(f"{filename}: I/O Error - {str(e)}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process JSON files")
    parser.add_argument("--json_files", nargs="+", help="JSON file names")
    args = parser.parse_args()

    json_files = args.json_files
    check_json_files(json_files)
