/*
Implemented v_store_daily_activities View with CASE, WINDOW, and CTE.

Developed the SQL view v_store_daily_activities, utilizing CASE statements, WINDOW functions, and Common Table Expressions (CTE) 
to analyze daily store activities, providing nuanced insights into conversion rates, purchases, and cumulative metrics.
*/
CREATE OR REPLACE VIEW v_store_daily_activities AS

WITH

main as (
  SELECT
  DATE(orders.created) as date_key,
  warungs_visited,
  warungs_registered,
  warungs_registered/warungs_visited as warungs_conversion,
  purchases_made,
  purchasing_stores,
  purchases_numbers,
  COUNT(DISTINCT orders.id) as orders,
  SUM(orders.total) as sales_numbers,
  COUNT(DISTINCT orders.store_id) as selling_stores
  FROM orders
  LEFT JOIN (
    SELECT
    DATE(created) as date_key,
    COUNT(DISTINCT id) as warungs_visited,
    COUNT(DISTINCT CASE WHEN STATUS = 'yes' THEN id END) as warungs_registered
    FROM psc_visits
    GROUP BY 1
  ) psc_visits on DATE(orders.created) = psc_visits.date_key
  LEFT JOIN (
    SELECT
    DATE(created) as date_key,
    COUNT(DISTINCT code) as purchases_made,
    COUNT(DISTINCT store_id) as purchasing_stores,
    SUM(total) purchases_numbers
    FROM purchases
    GROUP BY 1
  ) purchases on DATE(orders.created) = purchases.date_key
  WHERE date(orders.created) >= '2023-10-01'
  AND store_id in (SELECT distinct id FROM v_stores)
  GROUP BY 1,2,3,4,5,6,7
),

stores_ as (
  SELECT
  date_key,
  COUNT(DISTINCT store_id ) as total_store
  FROM (
      SELECT
      store_id,
      MIN(date(orders.created)) as date_key
      FROM orders
      WHERE date(orders.created) >= '2023-10-01'
      AND store_id in (SELECT distinct id FROM v_stores)
      GROUP BY 1
  ) a
  GROUP BY 1
)

SELECT
main.*,
SUM(orders) OVER (ORDER BY date_key) AS cumulative_orders,
SUM(sales_numbers) OVER (ORDER BY date_key) AS cumulative_sales_numbers,
SUM(total_store) OVER (ORDER BY date_key) AS cumulative_unique_stores
FROM main
LEFT JOIN stores_ ON main.date_key = stores_.date_key
