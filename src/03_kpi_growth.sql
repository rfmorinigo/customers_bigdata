-- KPI CRECIMIENTO

-- 1) Crecimiento mensual
SELECT
  subscription_year,
  subscription_month,
  COUNT(*) AS new_customers
FROM read_parquet('data/silver/customers_dedup.parquet')
GROUP BY 1,2
ORDER BY 1,2;

-- 2) Crecimiento anual
SELECT
  subscription_year,
  COUNT(*) AS new_customers
FROM read_parquet('data/silver/customers_dedup.parquet')
GROUP BY subscription_year
ORDER BY subscription_year;
