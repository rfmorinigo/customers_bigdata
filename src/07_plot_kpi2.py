import duckdb
import matplotlib.pyplot as plt
from pathlib import Path

OUT = Path("outputs/figures")
OUT.mkdir(parents=True, exist_ok=True)

query = """
SELECT
  country,
  COUNT(*) AS customers
FROM read_parquet('data/silver/customers_dedup.parquet')
WHERE country IS NOT NULL
GROUP BY country
ORDER BY customers DESC
LIMIT 10
"""

df = duckdb.sql(query).df()

plt.figure()
plt.barh(df["country"], df["customers"])
plt.title("Top 10 países por cantidad de clientes")
plt.xlabel("Clientes")
plt.ylabel("País")
plt.gca().invert_yaxis()
plt.tight_layout()

plt.savefig(OUT / "kpi2_top_countries.png")
plt.close()
