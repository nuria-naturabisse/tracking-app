import sqlite3, pandas as pd

conn = sqlite3.connect("events.db")
df = pd.read_sql("SELECT * FROM events ORDER BY client_id, event_time", conn)
conn.close()

# Agrupa eventos por cliente → construye rutas
paths = df.groupby("client_id").apply(
    lambda g: " > ".join(g["utm_source"].fillna("direct"))
).reset_index(name="path")

print(paths.head())
