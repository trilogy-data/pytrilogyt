-- Generated from preql source: C:\Users\ethan\coding_projects\pypreql-etl\jaffle_shop\models\example\customer.preql
--Do not edit manually
{{ config(materialized='table') }}

WITH 
exuberant as (
SELECT
    current_datetime() as `_preqlt__created_at`

),
level as (
SELECT
    orders_orders_raw.`id` as `orders_id`,
    orders_orders_raw.`order_date` as `orders_date`,
    orders_orders_raw.`user_id` as `customer_id`,
    orders_orders_raw.`status` as `orders_status`
FROM
    dbt-tutorial.jaffle_shop.orders as orders_orders_raw
),
darter as (
SELECT
    customer_customers_raw.`id` as `customer_id`,
    customer_customers_raw.`first_name` as `customer_first_name`,
    customer_customers_raw.`last_name` as `customer_last_name`
FROM
    dbt-tutorial.jaffle_shop.customers as customer_customers_raw
),
loon as (
SELECT
    level.`orders_id` as `orders_id`,
    darter.`customer_id` as `customer_id`,
    level.`orders_date` as `orders_date`
FROM
    darter as darter

LEFT OUTER JOIN level on darter.`customer_id` = level.`customer_id`

),
quick as (
SELECT
    count(loon.`orders_id`) as `customer_raw_number_of_orders`,
    loon.`customer_id` as `customer_id`,
    min(loon.`orders_date`) as `customer_first_order_date`,
    max(loon.`orders_date`) as `customer_most_recent_order_date`
FROM
    loon as loon
GROUP BY 
    loon.`customer_id`),
nondescript as (
SELECT
    coalesce(quick.`customer_raw_number_of_orders`,0) as `customer_number_of_orders`,
    quick.`customer_id` as `customer_id`
FROM
    quick as quick
),
mamba as (
SELECT
    quick.`customer_id` as `customer_id`,
    darter.`customer_first_name` as `customer_first_name`,
    darter.`customer_last_name` as `customer_last_name`,
    quick.`customer_first_order_date` as `customer_first_order_date`,
    quick.`customer_most_recent_order_date` as `customer_most_recent_order_date`,
    nondescript.`customer_number_of_orders` as `customer_number_of_orders`,
    exuberant.`_preqlt__created_at` as `_preqlt__created_at`
FROM
    quick as quick

LEFT OUTER JOIN darter on quick.`customer_id` = darter.`customer_id`

LEFT OUTER JOIN nondescript on quick.`customer_id` = nondescript.`customer_id`

FULL JOIN exuberant on 1=1

)
SELECT
    mamba.`customer_id`,
    mamba.`customer_first_name`,
    mamba.`customer_last_name`,
    mamba.`customer_first_order_date`,
    mamba.`customer_most_recent_order_date`,
    mamba.`customer_number_of_orders`,
    mamba.`_preqlt__created_at`
FROM
    mamba
