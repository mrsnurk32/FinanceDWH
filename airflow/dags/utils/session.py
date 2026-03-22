import requests

def get_http_session():
    session = requests.Session()
    session.headers.update({"Connection": "keep-alive"})
    return session