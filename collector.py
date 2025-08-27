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


# 🔹 NUEVO ENDPOINT para listar eventos
@app.get("/events")
def list_events():
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("""
        SELECT event_name, client_id, utm_source, utm_medium, utm_campaign, value, event_time 
        FROM events 
        ORDER BY event_time DESC 
        LIMIT 20
    """)
    rows = c.fetchall()
    conn.close()
    # lo devolvemos como lista de dicts para que se vea bonito en JSON
    return [
        {
            "event_name": r[0],
            "client_id": r[1],
            "utm_source": r[2],
            "utm_medium": r[3],
            "utm_campaign": r[4],
            "value": r[5],
            "event_time": r[6]
        }
        for r in rows
    ]
@app.get("/stats")
def stats():
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    # total de eventos
    c.execute("SELECT COUNT(*) FROM events")
    total_events = c.fetchone()[0]

    # total de revenue (suma de value)
    c.execute("SELECT SUM(value) FROM events")
    total_revenue = c.fetchone()[0]

    # breakdown por utm_source
    c.execute("SELECT utm_source, COUNT(*) as events, SUM(value) as revenue FROM events GROUP BY utm_source")
    rows = c.fetchall()
    conn.close()

    return {
        "total_events": total_events,
        "total_revenue": total_revenue,
        "by_channel": [
            {"utm_source": r[0], "events": r[1], "revenue": r[2]} for r in rows
        ]
    }
