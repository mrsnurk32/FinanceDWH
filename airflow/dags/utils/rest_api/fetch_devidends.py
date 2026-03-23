import requests
from utils.utils.convert_dt import localize_date

COLUMNS = (
    "secid",
    "isin",
    "registryclosedate",
    "value",
    "currencyid"
)

def parse_candles_data(record: dict) -> list:
    record['value'] = float(record['value'])
    record['registryclosedate'] = localize_date(record['registryclosedate'])
    return [record[col] for col in COLUMNS]


def fetch_moex_dividends(security: str) -> list[dict] | None:
    global COLUMNS
    resp = requests.get(
        f"https://iss.moex.com/iss/securities/{security}/dividends.json")
    
    if not resp.ok:
        print(f"Failed to fetch dividends for {security}")
        return None

    data = resp.json()
    rows = data["dividends"]["data"]
    columns = data["dividends"]["columns"]

    if len(rows):
        rows = [parse_candles_data(dict(zip(columns, row))) for row in rows]

    return rows, COLUMNS