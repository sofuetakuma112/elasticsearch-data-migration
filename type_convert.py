def convert_to_float(value):
    try:
        if value is None or value == "":
            return None
        else:
            return float(value)
    except ValueError:
        return None
