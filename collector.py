from fastapi import FastAPI, Request
import sqlite3, json, datetime

app = FastAPI()

DB_FILE = "events.db"

# inicializa la tabla
def init_db():
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_name TEXT,
            client_id TEXT,
            user_id TEXT,
            session_id TEXT,
            utm_source TEXT,
            utm_medium TEXT,
            utm_campaign TEXT,
            page_url TEXT,
            event_time TEXT,
            value REAL
        )
    """)
    conn.commit()
    conn.close()

init_db()

@app.post("/collect")
async def collect_event(request: Request):
    data = await request.json()
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("""
        INSERT INTO events (event_name, client_id, user_id, session_id,
        utm_source, utm_medium, utm_campaign, page_url, event_time, value)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        data.get("event_name"),
        data.get("client_id"),
        data.get("user_id"),
        data.get("session_id"),
        data.get("utm_source"),
        data.get("utm_medium"),
        data.get("utm_campaign"),
        data.get("page_url"),
        data.get("event_time", datetime.datetime.utcnow().isoformat()),
        data.get("value")
    ))
    conn.commit()
    conn.close()
    return {"status": "ok"}
