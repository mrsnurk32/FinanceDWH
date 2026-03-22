from datetime import datetime


QUERY = """
SELECT 
	s.secid AS secid
	, toTimeZone(max(begin), 'Europe/Moscow') AS last_record
FROM 
	source.securities AS s
LEFT JOIN source.market_data AS md
	ON md.secid = s.secid  
GROUP BY 
	secid
"""

def get_last_record_for_secid(secid, last_records) -> str | None:
    last_record:datetime = last_records.get(secid, None)
    if last_record is not None:
        # last_record_dt = last_record.strftime('%Y-%m-%d %H:%M:%S')
        return last_record
    else:
        return None

def fetch_last_security_records(client) -> dict:
    return dict(client.query(QUERY).result_rows)