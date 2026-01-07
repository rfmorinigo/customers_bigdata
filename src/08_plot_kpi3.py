import duckdb
import matplotlib.pyplot as plt
from pathlib import Path

OUT = Path("outputs/figures")
OUT.mkdir(parents=True, exist_ok=True)

query = """
SELECT
  email_domain,
  COUNT(*) AS customers
FROM read_parquet('data/silver/customers_dedup.parquet')
GROUP BY email_domain
ORDER BY customers DESC
LIMIT 10
"""

df = duckdb.sql(query).df()

plt.figure()
plt.pie(
    df["customers"],
    labels=df["email_domain"],
    autopct="%1.1f%%",
    startangle=90
)
plt.title("Distribución de dominios de email (Top 10)")
plt.tight_layout()

plt.savefig(OUT / "kpi3_email_domains_pie.png")
plt.close()
