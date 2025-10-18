{{ config(materialized='table') }}

SELECT
  branch_id,
  ROUND(AVG(revenue), 2) AS avg_revenue,
  ROUND(AVG(service_time), 2) AS avg_service_time,
  ROUND(AVG(rating), 2) AS avg_rating
FROM {{ source('restaurant_raw', 'performance') }}
GROUP BY branch_id
