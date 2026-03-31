from datetime import datetime


QUERY = """
SELECT 
	toTimeZone(max(cb.date), 'Europe/Moscow') AS last_record_dt
FROM 
	source.centralbank_rates_ru AS cb FINAL
"""

def fetch_last_rates_dt(client) -> dict:
    return client.query(QUERY).result_rows[0][0]