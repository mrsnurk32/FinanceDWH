import requests
from typing import List, Tuple, Any
import logging
from utils.utils.convert_dt import localize_datetime_cb


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

COLUMNS = ['date', 'periodicity', 'obs_val']  # Assumed correct keys; verify API response


def parse_result(item: dict[str, Any]) -> List[Any]:
    """Parse single item, converting obs_value to float."""
    item['date'] = localize_datetime_cb(item['date'])  # Ensure date is included
    try:
        item['obs_val'] = float(item.get('obs_val', 0))  # Use 'obs_val'; fallback to 0
    except (ValueError, TypeError) as e:
        logger.warning(f"Invalid obs_val {item.get('obs_val')}: {e}")
        item['obs_val'] = 0.0
    return [item.get(col) for col in COLUMNS]


def fetch_data(start_year: int, end_year: int) -> Tuple[List[List[Any]], List[str]]:
    """
    Fetch CBR data for given years.

    Args:
        start_year: Start year (e.g., 2020).
        end_year: End year (e.g., 2025).

    Returns:
        Tuple of (data rows, columns).

    Raises:
        ValueError: Invalid years.
        requests.RequestException: API errors.
    """
    if not (1900 <= int(start_year) <= int(end_year) <= 3100):
        raise ValueError("Years must be between 1900-2100 and start <= end.")

    base_url = "https://www.cbr.ru/dataservice/data"
    params = {
        "y1": start_year,
        "y2": end_year,
        "publicationId": 14,
        "datasetId": 27,
        "measureId": 2
    }

    try:
        resp = requests.get(base_url, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        raw_data = data.get('RawData', [])
        if not raw_data:
            logger.warning("No RawData in response")
            return [], COLUMNS

        results = [item for item in raw_data if item.get('colId') == 11]
        if not results:
            logger.warning("No items with colId=11")

        parsed_data = [parse_result(item) for item in results]
        return parsed_data, COLUMNS

    except requests.RequestException as e:
        logger.error(f"API request failed: {e}")
        raise