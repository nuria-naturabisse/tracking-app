import pandas as pd

# Ejemplo de rutas con conversiones
data = [
    {"client_id":"1", "path":"google > email > direct", "converted":1},
    {"client_id":"2", "path":"facebook > google", "converted":1},
    {"client_id":"3", "path":"email > direct", "converted":0},
]

df = pd.DataFrame(data)

def last_click(row):
    return row["path"].split(" > ")[-1] if row["converted"] else None

def first_click(row):
    return row["path"].split(" > ")[0] if row["converted"] else None

df["last"] = df.apply(last_click, axis=1)
df["first"] = df.apply(first_click, axis=1)

print("Last click:\n", df["last"].value_counts())
print("First click:\n", df["first"].value_counts())
