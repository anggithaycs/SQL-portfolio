CREATE OR REPLACE VIEW `anggi.personal.v_dsc_market_share` AS
WITH
sales as (
SELECT
sum(MntWines) as rev_wines,
sum(MntFruits) as rev_fruits,
sum(MntMeatProducts) as rev_meat,
sum(MntFishProducts) as rev_fish,
sum(MntSweetProducts) as rev_sweet,
sum(MntGoldProds) as rev_gold
FROM `anggi.personal.sdc_dataset`
),
main as (
SELECT 'Wine' as category, rev_wines as revenue_generated FROM sales
UNION ALL
SELECT 'Fruits' as category, rev_fruits as revenue_generated FROM sales
UNION ALL
SELECT 'Meats' as category, rev_meat as revenue_generated FROM sales
UNION ALL
SELECT 'Fishes' as category, rev_fish as revenue_generated FROM sales
UNION ALL
SELECT 'Sweets' as category, rev_sweet as revenue_generated FROM sales
UNION ALL
SELECT 'Gold' as category, rev_gold as revenue_generated FROM sales
)
SELECT * FROM main

/*2. View Campaign Participation*/
CREATE OR REPLACE VIEW `anggi.personal.v_dsc_campaign_participation` AS
WITH
campaign as (
SELECT
sum(AcceptedCmp1) as accept_campaign_1,
sum(AcceptedCmp2) as accept_campaign_2,
sum(AcceptedCmp3) as accept_campaign_3,
sum(AcceptedCmp4) as accept_campaign_4,
sum(AcceptedCmp5) as accept_campaign_5,
sum(Response) as accept_campaign_6
FROM `anggi.personal.sdc_dataset`
),
main as (
SELECT 'Campaign 1' as campaign_mark, accept_campaign_1 as campaign_participant FROM campaign
UNION ALL
SELECT 'Campaign 2' as campaign_mark, accept_campaign_2 as campaign_participant FROM campaign
UNION ALL
SELECT 'Campaign 3' as campaign_mark, accept_campaign_3 as campaign_participant FROM campaign
UNION ALL
SELECT 'Campaign 4' as campaign_mark, accept_campaign_4 as campaign_participant FROM campaign
UNION ALL
SELECT 'Campaign 5' as campaign_mark, accept_campaign_5 as campaign_participant FROM campaign
UNION ALL
SELECT 'Campaign 6' as campaign_mark, accept_campaign_6 as campaign_participant FROM campaign
)
SELECT * FROM main

/*3. View Sales Channel*/
CREATE OR REPLACE VIEW `anggi.personal.v_dsc_sales_channel` AS
WITH
trx as (
SELECT
sum(NumWebPurchases) as trx_web,
sum(NumCatalogPurchases) as trx_catalog,
sum(NumStorePurchases) as trx_store
FROM `anggi.personal.sdc_dataset`
),
main as (
SELECT 'Web' as channel, trx_web as trx_channel FROM trx
UNION ALL
SELECT 'Catalog' as channel, trx_catalog as trx_channel FROM trx
UNION ALL
SELECT 'Store' as channel, trx_store as trx_channel FROM trx
)
SELECT * FROM main

/*4. View Percentile Dictionary*/
CREATE OR REPLACE VIEW `anggi.personal.v_dsc_percentile_dict` AS
SELECT
DISTINCT
PERCENTILE_CONT(Year_Birth, 0.25) OVER () AS p25_birthyear,
PERCENTILE_CONT(Year_Birth, 0.5) OVER () AS p50_birthyear,
PERCENTILE_CONT(Year_Birth, 0.75) OVER () AS p75_birthyear,
PERCENTILE_CONT(Income, 0.25) OVER () AS p25_income,
PERCENTILE_CONT(Income, 0.5) OVER () AS p50_income,
PERCENTILE_CONT(Income, 0.75) OVER () AS p75_income
FROM `anggi.personal.sdc_dataset`

/*5. View SDC Dataset*/
SELECT
a.*,
CASE
WHEN a.year_birth < b.p25_birthyear THEN 'Cat 1: Before 1958'
WHEN a.year_birth >= b.p25_birthyear AND a.year_birth < b.p50_birthyear THEN 'Cat 2: 1959 - 1969'
WHEN a.year_birth >= b.p50_birthyear AND a.year_birth < b.p75_birthyear THEN 'Cat 3: 1970 - 1976'
WHEN a.year_birth >= b.p75_birthyear THEN 'Cat 4: After 1977'
END as birth_year_category,
CASE
WHEN a.income < b.p25_income THEN 'Cat 1: Less than P25'
WHEN a.income >= b.p25_income AND a.income < b.p50_income THEN 'Cat 2: Less than P50'
WHEN a.income >= b.p50_income AND a.income < b.p75_income THEN 'Cat 3: Less than P75'
WHEN a.income >= b.p75_income THEN 'Cat 4: More than P75'
END as income_category,
CASE
WHEN kidhome = 0 and teenhome = 0 then 'No'
ELSE 'Yes'
END has_kid_teen,

AcceptedCmp1 + AcceptedCmp2 + AcceptedCmp3 + AcceptedCmp4 + AcceptedCmp5 + Response as campaign_accepted,
  
CASE
WHEN AcceptedCmp1 + AcceptedCmp2 + AcceptedCmp3 + AcceptedCmp4 + AcceptedCmp5 + Response > 0 THEN 'Yes'
ELSE 'No'
END as campaign_flag,
CASE
WHEN Complain = 0 THEN 'No'
ELSE 'Yes'
END as complain_flag,
CASE
WHEN NumDealsPurchases = 0 THEN 'No'
ELSE 'Yes'
END as discount_flag,
NumWebPurchases + NumCatalogPurchases + NumStorePurchases as number_of_purchases,
MntWines + MntFruits + MntMeatProducts + MntFishProducts + MntSweetProducts + MntGoldProds as total_spending
FROM `anggi.personal.sdc_dataset` a
CROSS JOIN `anggi.personal.v_dsc_percentile_dict` b
