from fastapi import FastAPI, Request, HTTPException
from pydantic import BaseModel
from typing import Optional
import sqlite3, datetime, json

app = FastAPI()

DB_FILE = "events.db"

# ---------- Modelo de entrada (validación) ----------
class EventIn(BaseModel):
    event_name: Optional[str] = None
    client_id: Optional[str] = None
    user_id: Optional[str] = None
    session_id: Optional[str] = None
    utm_source: Optional[str] = None
    utm_medium: Optional[str] = None
    utm_campaign: Optional[str] = None
    page_url: Optional[str] = None
    event_time: Optional[str] = None
    value: Optional[float] = None

def get_conn():
    # permite uso desde varios hilos (Render/uvicorn)
    return sqlite3.connect(DB_FILE, check_same_thread=False)

# ---------- Init DB ----------
def init_db():
    conn = get_conn()
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

# ---------- Colector ----------
@app.post("/collect")
async def collect_event(request: Request):
    try:
        raw = await request.body()
        if not raw or raw.strip() in (b"",):
            raise HTTPException(status_code=400, detail="Empty body: send JSON")
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            raise HTTPException(status_code=400, detail="Invalid JSON")

        # valida/normaliza con Pydantic
        evt = EventIn(**payload)
        event_time = evt.event_time or datetime.datetime.utcnow().isoformat()

        conn = get_conn()
        c = conn.cursor()
        c.execute("""
            INSERT INTO events (
                event_name, client_id, user_id, session_id,
                utm_source, utm_medium, utm_campaign, page_url, event_time, value
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            evt.event_name,
            evt.client_id,
            evt.user_id,
            evt.session_id,
            evt.utm_source,
            evt.utm_medium,
            evt.utm_campaign,
            evt.page_url,
            event_time,
            evt.value
        ))
        conn.commit()
        conn.close()
        return {"status": "ok"}
    except HTTPException:
        raise
    except Exception as e:
        # para depurar en Render logs
        return HTTPException(status_code=500, detail=f"Server error: {type(e).__name__}")

# ---------- Listado simple ----------
@app.get("/events")
def list_events():
    conn = get_conn()
    c = conn.cursor()
    c.execute("""
        SELECT event_name, client_id, utm_source, utm_medium, utm_campaign, value, event_time
        FROM events ORDER BY event_time DESC LIMIT 50
    """)
    rows = c.fetchall()
    conn.close()
    return [
        {
            "event_name": r[0],
            "client_id": r[1],
            "utm_source": r[2],
            "utm_medium": r[3],
            "utm_campaign": r[4],
            "value": r[5],
            "event_time": r[6]
        } for r in rows
    ]

# ---------- Stats rápidas ----------
@app.get("/stats")
def stats():
    conn = get_conn()
    c = conn.cursor()
    c.execute("SELECT COUNT(*), SUM(value) FROM events")
    total_events, total_revenue = c.fetchone()
    c.execute("""
        SELECT COALESCE(utm_source,'(none)'), COUNT(*) AS events, SUM(value) AS revenue
        FROM events GROUP BY utm_source
        ORDER BY events DESC
    """)
    rows = c.fetchall()
    conn.close()
    return {
        "total_events": total_events or 0,
        "total_revenue": total_revenue or 0,
        "by_channel": [{"utm_source": r[0], "events": r[1], "revenue": r[2]} for r in rows]
    }
