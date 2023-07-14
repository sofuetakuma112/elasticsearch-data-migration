import json


def process_json_lines(file_path):
    with open(file_path, "r") as file:
        for line in file:
            data = json.loads(line)
            yield data
