from tqdm import tqdm
from constants import DUMPED_JSON_FILE_PATH
from utils.datetime import parse_utctime
from utils.json import process_json_lines


if __name__ == "__main__":
    utctime_set = set()
    duplicate_count = 0

    oldest_utctime = None
    newest_utctime = None

    docs = process_json_lines(DUMPED_JSON_FILE_PATH)

    for doc in tqdm(docs, desc="Processing"):
        if "_source" in doc and "utctime" in doc["_source"]:
            utctime = doc["_source"]["utctime"]
            if utctime in utctime_set:
                duplicate_count += 1
            else:
                utctime_set.add(utctime)
                parsed_utctime = parse_utctime(utctime)
                if oldest_utctime is None or parsed_utctime < oldest_utctime:
                    oldest_utctime = parsed_utctime
                if newest_utctime is None or parsed_utctime > newest_utctime:
                    newest_utctime = parsed_utctime
        else:
            raise Exception("不正なドキュメントデータ")

    print("Duplicate utctime count:", duplicate_count)
    print("Oldest utctime:", oldest_utctime)
    print("Newest utctime:", newest_utctime)
