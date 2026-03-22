import requests
from typing import Optional

from utils.utils.convert_dt import localize_date

COLUMNS = [
    'secid',
    'boardid',
    'shortname',
    'prevprice',
    'lotsize',
    'facevalue',
    'status',
    'boardname',
    'decimals',
    'secname',
    'remarks',
    'marketcode',
    'instrid',
    'sectorid',
    'minstep',
    'prevwaprice',
    'faceunit',
    'prevdate',
    'issuesize',
    'isin',
    'latname',
    'regnumber',
    'prevlegalcloseprice',
    'currencyid',
    'sectype',
    'listlevel',
    'settledate'
]

def parse(record: dict) -> list[str]:
    record['prevdate'] = localize_date(record['prevdate']) \
                            if record['prevdate'] != '0000-00-00' else None
    record['settledate'] = localize_date(record['settledate']) \
                            if record['settledate'] != '0000-00-00' else None
    return [record[col] for col in COLUMNS]

def fetch_moex_securities() -> Optional[tuple[list, list[str]]]:
    global COLUMNS
    resp = requests.get(
        "https://iss.moex.com/iss/engines/stock/markets/shares/securities.json")
    
    if not resp.ok:
        print(f"Failed to fetch security")
        return None

    data = resp.json()['securities']
    columns = [col.lower() for col in data['columns']]

    rows = data["data"]

    data = [parse(dict(zip(columns, row))) for row in rows]

    return data, COLUMNS