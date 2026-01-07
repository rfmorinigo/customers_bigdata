 -- KPI CALIDAD DE DATOS

-- 1) Impacto de deduplicación
SELECT
  2000000 AS rows_bronze,
  COUNT(*) AS rows_silver,
  2000000 - COUNT(*) AS duplicates_removed
FROM read_parquet('data/silver/customers_dedup.parquet');


-- 2) % clientes sin empresa
SELECT
  COUNT(*) FILTER (WHERE company IS NULL OR company = '') * 100.0 / COUNT(*) 
    AS pct_without_company
FROM read_parquet('data/silver/customers_dedup.parquet');


-- 3) Dominios de email (Top 10)
SELECT
  email_domain,
  COUNT(*) AS customers
FROM read_parquet('data/silver/customers_dedup.parquet')
GROUP BY email_domain
ORDER BY customers DESC
LIMIT 10;
