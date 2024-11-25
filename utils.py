from datetime import datetime

def validate_schema(data, keys, key_types):
    for k in keys:
        # Check if key exists or key is the right type
        if k not in data or not isinstance(data[k], key_types[k]):
            return False
        # Check if value is null
        if data[k] is None:
            return False
    return True

def validate_time(data):
    try:
        datetime.fromisoformat(data["timestamp"].replace("Z", "+00:00"))
        return True
    except ValueError as e:
        print(e)
        return False

    