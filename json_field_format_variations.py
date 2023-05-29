import json
import os
from tqdm import tqdm
from utils.constants import JSON_DIR_FULL_PATH
from utils.lib import read_json_file_line_by_line, get_json_file_line_count
from utils.type_convert import convert_to_float


def is_nan(value):
    return value is not None and isinstance(value, float) and value != value

OUTPUT_FILE = "out/nan_values.json"

def main(directory, target_json_files=[]):
    with open(OUTPUT_FILE, "r") as f:
        data = json.load(f)

    invalid_temps = data["invalid_temps"]
    invalid_rhs = data["invalid_rhs"]
    invalid_ppms = data["invalid_ppms"]
    invalid_temperatures = data["invalid_temperatures"]
    processed_files = data["processed_files"]

    if not target_json_files:
        target_json_files = os.listdir(directory)

    for file_name in tqdm(os.listdir(directory), desc="Processing Files", unit="file"):
        if not file_name.endswith(".json"):
            continue

        if not file_name in target_json_files:
            continue

        if file_name in processed_files:
            continue

        file_path = os.path.join(directory, file_name)

        file_lines = read_json_file_line_by_line(file_path)
        total_lines = get_json_file_line_count(file_path)

        for doc in tqdm(
            file_lines,
            total=total_lines,
            desc=file_name,
            unit="line",
        ):
            source = doc["_source"]

            temp = source.get("TEMP")
            rh = source.get("RH")
            ppm = source.get("PPM")
            temperature = source.get("Temperature")

            temp_float = convert_to_float(temp)
            rh_float = convert_to_float(rh)
            ppm_float = convert_to_float(ppm)
            temperature_float = convert_to_float(temperature)

            if is_nan(temp_float):
                invalid_temps.append(temp)
            if is_nan(rh_float):
                invalid_rhs.append(rh)
            if is_nan(ppm_float):
                invalid_ppms.append(ppm)
            if is_nan(temperature_float):
                invalid_temperatures.append(temperature)

            processed_files.append(file_name)

        with open(OUTPUT_FILE, "w") as f:
            f.write(
                json.dumps(
                    {
                        "invalid_temps": invalid_temps,
                        "invalid_rhs": invalid_rhs,
                        "invalid_ppms": invalid_ppms,
                        "invalid_temperatures": invalid_temperatures,
                        "processed_files": processed_files,
                    }
                )
                + "\n"
            )


if __name__ == "__main__":
    main(JSON_DIR_FULL_PATH)
