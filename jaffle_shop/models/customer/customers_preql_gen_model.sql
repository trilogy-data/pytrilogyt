-- Generated from preql source: C:\Users\ethan\coding_projects\pypreql-etl\jaffle_shop\models\example\customer.preql
--Do not edit manually
{{ config(materialized='table') }}

WITH 
quick as (
SELECT
    current_datetime() as `_preqlt__created_at`

),
vivacious as (
SELECT
    orders_orders_raw.`id` as `orders_id`,
    orders_orders_raw.`order_date` as `orders_date`,
    orders_orders_raw.`user_id` as `customer_id`,
    orders_orders_raw.`status` as `orders_status`
FROM
    dbt-tutorial.jaffle_shop.orders as orders_orders_raw
),
dove as (
SELECT
    customer_customers_raw.`id` as `customer_id`,
    customer_customers_raw.`first_name` as `customer_first_name`,
    customer_customers_raw.`last_name` as `customer_last_name`
FROM
    dbt-tutorial.jaffle_shop.customers as customer_customers_raw
),
uneven as (
SELECT
    vivacious.`orders_id` as `orders_id`,
    dove.`customer_id` as `customer_id`,
    vivacious.`orders_date` as `orders_date`
FROM
    dove as dove

LEFT OUTER JOIN vivacious on dove.`customer_id` = vivacious.`customer_id`

),
imported as (
SELECT
    count(uneven.`orders_id`) as `customer_raw_number_of_orders`,
    uneven.`customer_id` as `customer_id`,
    min(uneven.`orders_date`) as `customer_first_order_date`,
    max(uneven.`orders_date`) as `customer_most_recent_order_date`
FROM
    uneven as uneven
GROUP BY 
    uneven.`customer_id`),
turkey as (
SELECT
    coalesce(imported.`customer_raw_number_of_orders`,0) as `customer_number_of_orders`,
    imported.`customer_id` as `customer_id`
FROM
    imported as imported
),
macho as (
SELECT
    imported.`customer_id` as `customer_id`,
    dove.`customer_first_name` as `customer_first_name`,
    dove.`customer_last_name` as `customer_last_name`,
    imported.`customer_first_order_date` as `customer_first_order_date`,
    imported.`customer_most_recent_order_date` as `customer_most_recent_order_date`,
    turkey.`customer_number_of_orders` as `customer_number_of_orders`,
    quick.`_preqlt__created_at` as `_preqlt__created_at`
FROM
    imported as imported

LEFT OUTER JOIN dove on imported.`customer_id` = dove.`customer_id`

LEFT OUTER JOIN turkey on imported.`customer_id` = turkey.`customer_id`

FULL JOIN quick on 1=1

)
SELECT
    macho.`customer_id`,
    macho.`customer_first_name`,
    macho.`customer_last_name`,
    macho.`customer_first_order_date`,
    macho.`customer_most_recent_order_date`,
    macho.`customer_number_of_orders`,
    macho.`_preqlt__created_at`
FROM
    macho
