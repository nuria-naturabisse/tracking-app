import pandas as pd
import networkx as nx

# Datos de ejemplo
data = [
    {"path":"google > email > direct", "converted":1},
    {"path":"facebook > google", "converted":1},
    {"path":"email > direct", "converted":0},
]

df = pd.DataFrame(data)

# Construir transiciones
G = nx.DiGraph()
for _, row in df.iterrows():
    states = ["start"] + row["path"].split(" > ") + (["conversion"] if row["converted"] else ["null"])
    for i in range(len(states)-1):
        a, b = states[i], states[i+1]
        G.add_edge(a, b, weight=G.get_edge_data(a, b, {"weight":0})["weight"] + 1)

# Normalizar pesos → probabilidades
for a, b in G.edges:
    total = sum(d["weight"] for _, _, d in G.out_edges(a, data=True))
    G[a][b]["prob"] = G[a][b]["weight"] / total

print("Transiciones Markov:")
for a, b, d in G.edges(data=True):
    print(f"{a} -> {b}: {d['prob']:.2f}")
