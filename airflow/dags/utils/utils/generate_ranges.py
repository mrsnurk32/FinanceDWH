from datetime import date
from datetime import timedelta

import calendar

from typing import Optional
from typing import List
from typing import Dict


def generate_fetch_ranges(last_fetched_date: Optional[date], today_date: date) -> List[Dict[str, str]]:

    yesterday = today_date - timedelta(days=1)

    if last_fetched_date is None:
        start = date(2021, 1, 1)
    else:
        start = last_fetched_date + timedelta(days=1)

    if start > yesterday:
        return []

    ranges = []

    while start <= yesterday:

        last_day = calendar.monthrange(start.year, start.month)[1]
        month_end = date(start.year, start.month, last_day)

        end = min(month_end, yesterday)

        ranges.append({
            "start_date": start.isoformat(),
            "end_date": end.isoformat()
        })

        start = end + timedelta(days=1)

    return ranges