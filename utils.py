from datetime import datetime
import math

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

def calculate_distance(lat1, lon1, lat2, lon2):
    R = 6371  # Radius of the Earth in km
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    delta_phi = math.radians(lat2 - lat1)
    delta_lambda = math.radians(lon2 - lon1)

    a = math.sin(delta_phi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(delta_lambda / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

    return R * c  # Distance in km
    