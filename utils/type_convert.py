import numpy as np

def convert_to_float(value):
    if value is None or value == "":
        return None
    
    try:
        result = float(value)
        if np.isnan(result):
            return None
        else:
            return result
    except ValueError:
        return None