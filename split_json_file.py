import json
import os

from utils.lib import read_json_file_line_by_line


def split_json_file(file_path, target_file_size):
    file_name = os.path.basename(file_path)
    file_dir = os.path.dirname(file_path)

    output_file_index = 1
    output_file_path = os.path.join(file_dir, f"{file_name}_{output_file_index}.json")
    output_file_size = 0

    json_generator = read_json_file_line_by_line(file_path)

    with open(output_file_path, "w") as output_file:
        for json_data in json_generator:
            json_line = json.dumps(json_data) + "\n"
            line_size = len(json_line)

            if output_file_size + line_size > target_file_size:
                output_file_index += 1
                output_file_path = os.path.join(
                    file_dir, f"{file_name}_{output_file_index}.json"
                )
                output_file_size = 0
                output_file.close()

                output_file = open(output_file_path, "w")

            output_file.write(json_line)
            output_file_size += line_size

    print("JSON file split completed.")
