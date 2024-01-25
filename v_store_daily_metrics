/*
Implemented v_store_daily_metrics View with comprehensive metrics on daily store activities, 
including total orders, sales numbers, and purchase details. Utilized CTEs with JOINs, 
CASE statements, and aggregations to derive insightful performance metrics for each store.
*/
CREATE OR REPLACE VIEW v_store_daily_metrics AS 

WITH

ord as (
  SELECT
  DATE(orders.created) as date_key,
  orders.store_id,
  COUNT(DISTINCT orders.id) as total_order,
  SUM(orders.total) as sales_numbers
  FROM orders
  GROUP BY 1,2
),

pur as (
  SELECT
  DATE(purchases.created) as date_key,
  purchases.store_id,
  COUNT(DISTINCT purchases.code) as number_purchase,
  SUM(purchases.total_product) as total_sku_purchase,
  SUM(purchases.total_item) as total_volume_purchase,
  SUM(purchases.total) as total_purchase
  FROM purchases
  GROUP BY 1,2
),

success as (
  SELECT
  DATE(created) as date_key,
  store_id,
  COUNT(DISTINCT purchases_histories.code) as success_purchase
  FROM purchases_histories
  WHERE status = 'success'
  GROUP BY 1,2
),

main as (
  SELECT
  orders.date_key,
  orders.store_id,
  stores.psc_id,
  CONCAT(stores.name, '<br />ID: ', 'store-', stores.id) as store_name,
  stores.is_active,
  users_profiles.name as psc_name,
  orders.total_order,
  sales_numbers,
  number_purchase,
  total_sku_purchase,
  total_volume_purchase,
  total_purchase,
  success_purchase,
  COUNT(DISTINCT CASE WHEN so.store_id = orders.store_id and DATE(so.created) <= orders.date_key THEN so.id END) as supplier_added
  FROM ord orders
  LEFT JOIN stores on orders.store_id = stores.id
  LEFT JOIN users_profiles on stores.psc_id = users_profiles.user_id
  LEFT JOIN pur purchases on orders.store_id = purchases.store_id and orders.date_key = purchases.date_key
  LEFT JOIN suppliers_orders so on orders.store_id = so.store_id
  LEFT JOIN success on orders.store_id = success.store_id and orders.date_key = success.date_key
  WHERE orders.store_id IN (SELECT DISTINCT id FROM v_stores_achievement)
  GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12
)

SELECT
date_key,
store_id,
psc_id,
CONCAT(store_name, ' (', supplier_added, ')') store_name,
psc_name,
total_order,
sales_numbers,
number_purchase,
total_sku_purchase,
total_volume_purchase,
total_purchase,
success_purchase,
is_active
FROM main
WHERE date_key >= '2023-10-01'
ORDER BY 2,1 DESC
