from datetime import datetime
import pytz

MOSCOW_TZ = pytz.timezone("Europe/Moscow")
UTC_TZ = pytz.utc

def localize_datetime_cb(stamp: str) -> datetime:
    stamp_datetime = datetime.strptime(stamp, '%Y-%m-%dT%H:%M:%S')
    return MOSCOW_TZ.localize(stamp_datetime).astimezone(UTC_TZ)

def localize_datetime(stamp: str) -> datetime:
    stamp_datetime = datetime.strptime(stamp, '%Y-%m-%d %H:%M:%S')
    return MOSCOW_TZ.localize(stamp_datetime).astimezone(UTC_TZ)


def localize_date(stamp: str) -> datetime:
    stamp_datetime = datetime.strptime(stamp, '%Y-%m-%d')
    return MOSCOW_TZ.localize(stamp_datetime).astimezone(UTC_TZ)