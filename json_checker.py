import argparse
import os
import json
import subprocess
from constants import JSON_DIR_FULL_PATH
from tqdm import tqdm

from lib import get_json_file_line_count, read_json_file_line_by_line

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
    invalid_json_files = []
    if json_files is None:
        for filename in os.listdir(JSON_DIR_FULL_PATH):
            if filename.endswith(".json"):
                if is_valid_json_file_cached(filename):
                    continue
                has_error, error_indexes = check_json_file(JSON_DIR_FULL_PATH, filename)
                if has_error:
                    invalid_json_files.append(
                        {
                            "filename": filename,
                            "error_line_indexes": list(
                                map(lambda i: i + 1, error_indexes)
                            ),
                        }
                    )
    else:
        for filename in json_files:
            if is_valid_json_file_cached(filename):
                continue
            has_error, error_indexes = check_json_file(JSON_DIR_FULL_PATH, filename)
            if has_error:
                invalid_json_files.append(
                    {
                        "filename": filename,
                        "error_line_indexes": list(map(lambda i: i + 1, error_indexes)),
                    }
                )

    return invalid_json_files


def check_json_file(directory, filename):
    has_error = False
    error_indexes = []

    file_path = os.path.join(directory, filename)
    try:
        line_count = get_json_file_line_count(file_path)
        progress_bar = tqdm(total=line_count, desc=filename, unit="line")
        for index, obj in enumerate(read_json_file_line_by_line(file_path)):
            _ = obj
            progress_bar.update(1)
        progress_bar.close()
        print(f"{filename}: Valid JSON")
        cache_valid_json_file(filename)
    except ValueError as e:
        print(f"{filename}: Invalid JSON - {str(e)}")
        has_error = True
        error_indexes.append(index)
    except IOError as e:
        print(f"{filename}: I/O Error - {str(e)}")
        has_error = True
        error_indexes.append(index)

    return has_error, error_indexes


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process JSON files")
    parser.add_argument("--json_files", nargs="+", help="JSON file names")
    args = parser.parse_args()

    json_files = args.json_files
    invalid_json_files = check_json_files(json_files)

    print(f"invalid_json_files: {invalid_json_files}")
