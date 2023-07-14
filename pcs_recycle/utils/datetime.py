from datetime import datetime


def parse_utctime(utctime):
    if len(utctime) == 19:  # No microsecond component
        parsed_utctime = datetime.strptime(utctime, "%Y-%m-%dT%H:%M:%S")
    else:  # With microsecond component
        parsed_utctime = datetime.strptime(utctime, "%Y-%m-%dT%H:%M:%S.%f")
    return parsed_utctime
