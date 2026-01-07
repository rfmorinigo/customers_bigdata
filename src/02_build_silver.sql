COPY (
  SELECT
    index::INTEGER                    AS row_index,
    customer_id,
    lower(trim(first_name))           AS first_name,
    lower(trim(last_name))            AS last_name,
    trim(company)                     AS company,
    trim(city)                        AS city,
    trim(country)                     AS country,
    trim(phone_1)                     AS phone_1,
    trim(phone_2)                     AS phone_2,
    lower(trim(email))                AS email,
    split_part(lower(email), '@', 2)  AS email_domain,
    CAST(subscription_date AS DATE)   AS subscription_date,
    EXTRACT(year  FROM CAST(subscription_date AS DATE)) AS subscription_year,
    EXTRACT(month FROM CAST(subscription_date AS DATE)) AS subscription_month,
    trim(website)                     AS website
  FROM (
    SELECT *,
      ROW_NUMBER() OVER (
        PARTITION BY lower(trim(email))
        ORDER BY CAST(subscription_date AS DATE) DESC
      ) AS rn
    FROM read_parquet('data/bronze/customers_parquet/*.parquet')
    WHERE email IS NOT NULL
  )
  WHERE rn = 1
)
TO 'data/silver/customers_dedup.parquet'
(FORMAT PARQUET);
