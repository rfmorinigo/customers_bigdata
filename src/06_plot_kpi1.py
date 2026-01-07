import duckdb
import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path

OUT = Path("outputs/figures")
OUT.mkdir(parents=True, exist_ok=True)

query = """
SELECT
  subscription_year,
  subscription_month,
  COUNT(*) AS new_customers
FROM read_parquet('data/silver/customers_dedup.parquet')
GROUP BY subscription_year, subscription_month
ORDER BY subscription_year, subscription_month
"""

df = duckdb.sql(query).df()

df["date"] = pd.to_datetime(
    df["subscription_year"].astype(str) + "-" +
    df["subscription_month"].astype(str) + "-01"
)

plt.figure()
plt.plot(df["date"], df["new_customers"])
plt.title("Crecimiento mensual de clientes")
plt.xlabel("Fecha")
plt.ylabel("Clientes nuevos")
plt.xticks(rotation=45)
plt.tight_layout()

plt.savefig(OUT / "kpi1_growth_monthly.png")
plt.close()
