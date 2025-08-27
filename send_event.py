import requests, datetime

event = {
    "event_name": "purchase",
    "client_id": "abc123",
    "user_id": "u1",
    "session_id": "s1",
    "utm_source": "google",
    "utm_medium": "cpc",
    "utm_campaign": "summer_sale",
    "page_url": "https://example.com/product",
    "event_time": datetime.datetime.utcnow().isoformat(),
    "value": 120.0
}

r = requests.post("http://localhost:8000/collect", json=event)
print(r.json())
