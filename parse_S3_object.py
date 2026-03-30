"""
parse_s3_object.py
    Funkcja służy do parsowania ścieżek plików pochodzących z Kafki zapisanych w formacie S3.
    Oczekuje ścieżki w postaci:
        AIR/dn=<device>/date=<YYYY-MM-DD>/<HH>.parquet
    Na podstawie dopasowania regexem wydobywa nazwę telegramu, urządzenie oraz datę i godzinę,
    po czym zwraca te dane jako słownik:
        {
            "telegram": <str>,
            "device": <str>,
            "datetime": <datetime.datetime>
        }
    W przypadku niepoprawnego formatu ścieżki lub błędu konwersji daty — zgłaszany jest ValueError.
"""

import re
import datetime

def parseS3object(s3_object: str):
    """
    Parses a path in the format:
    AIR/dn=<device>/date=<YYYY-MM-DD>/<HH>.parquet
    Returns a dict with telegram, device, and datetime.
    """
    if not isinstance(s3_object, str):
        raise ValueError("s3_object must be a string")

    s3_object = s3_object.strip()

    # Regex matching pattern
    pattern = re.compile(
        r'^(?P<telegram>[^/]+)/(?P<device>[^/]+)/date=(?P<date>\d{4}-\d{2}-\d{2})/(?P<hour>\d{1,2})\.parquet$'
    )

    match = pattern.match(s3_object)
    if not match:
        raise ValueError(f"Path does not match expected pattern: {s3_object}")

    telegram = match.group("telegram")
    device = match.group("device")
    date_str = match.group("date")
    hour_str = match.group("hour")

    # Create datetime object
    try:
        date_part = datetime.datetime.strptime(date_str, "%Y-%m-%d").date()
        hour = int(hour_str)
        timestamp = datetime.datetime(date_part.year, date_part.month, date_part.day, hour, 0, 0)
    except Exception as e:
        raise ValueError(f"Error creating datetime from {date_str=} and {hour_str=}: {e}")

    return {
        "telegram": telegram,
        "device": device,
        "datetime": timestamp,
    }