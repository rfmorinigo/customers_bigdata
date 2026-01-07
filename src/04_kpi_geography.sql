-- KPI GEOGRAFÍA

-- 1) Clientes por país (Top 10)
SELECT
  country,
  COUNT(*) AS customers
FROM read_parquet('data/silver/customers_dedup.parquet')
WHERE country IS NOT NULL
GROUP BY country
ORDER BY customers DESC
LIMIT 10;

-- --------------------------------

-- 2) Porcentaje de clientes por país (Top 5)
WITH total AS (
  SELECT COUNT(*) AS total_customers
  FROM read_parquet('data/silver/customers_dedup.parquet')
)
SELECT
  country,
  COUNT(*) * 100.0 / (SELECT total_customers FROM total) AS pct_customers
FROM read_parquet('data/silver/customers_dedup.parquet')
GROUP BY country
ORDER BY pct_customers DESC
LIMIT 5;
