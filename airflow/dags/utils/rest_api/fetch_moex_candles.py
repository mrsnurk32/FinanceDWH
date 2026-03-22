import requests
from datetime import datetime
import pytz

MOSCOW_TZ = pytz.timezone("Europe/Moscow")
UTC_TZ = pytz.utc

COLUMNS = (
    "secid",
    "open",
    "close",
    "high",
    "low",
    "value",
    "volume",
    "begin",
    "end"
)


def localize_datetime(stamp: str) -> datetime:
    stamp_datetime = datetime.strptime(stamp, '%Y-%m-%d %H:%M:%S')
    return MOSCOW_TZ.localize(stamp_datetime).astimezone(UTC_TZ)


def parse_candles_data(record: dict) -> list:
    
    record['open'] = float(record['open'])
    record['high'] = float(record['high'])
    record['low'] = float(record['low'])
    record['close'] = float(record['close'])
    record['begin'] = localize_datetime(record['begin'])
    record['end'] = localize_datetime(record['end'])

    return [record[col] for col in COLUMNS]


def fetch_moex_candles(session: requests.Session , security: str, start_date: str, end_date: str) -> list[dict] | None:
    
    global COLUMNS

    resp = session.get(
        f"https://iss.moex.com/iss/engines/stock/markets/shares/securities/{security}/candles.json"
        , params = {
            "interval": 24,
            "from": start_date,
            "till": end_date,
            "iss.meta": "off"
        })
    
    if not resp.ok:
        print(f"Failed to fetch candles for {security}")
        return None

    data = resp.json()
    rows = data["candles"]["data"]
    data = [parse_candles_data(dict(zip(COLUMNS, [security] + row))) for row in rows]

    return data, COLUMNS