-- Generated from preql source: C:\Users\ethan\coding_projects\pypreql-etl\jaffle_shop\models\example\customer.preql
--Do not edit manually
{{ config(materialized='table') }}

WITH 
petrel as (
SELECT
    orders_orders_raw.`id` as `orders_id`,
    orders_orders_raw.`order_date` as `orders_date`,
    orders_orders_raw.`user_id` as `customer_id`,
    orders_orders_raw.`status` as `orders_status`
FROM
    dbt-tutorial.jaffle_shop.orders as orders_orders_raw
),
falcon as (
SELECT
    customer_customers_raw.`id` as `customer_id`,
    customer_customers_raw.`first_name` as `customer_first_name`,
    customer_customers_raw.`last_name` as `customer_last_name`
FROM
    dbt-tutorial.jaffle_shop.customers as customer_customers_raw
),
heron as (
SELECT
    petrel.`orders_id` as `orders_id`,
    falcon.`customer_id` as `customer_id`,
    petrel.`orders_date` as `orders_date`
FROM
    falcon as falcon

LEFT OUTER JOIN petrel on falcon.`customer_id` = petrel.`customer_id`

),
obsolete as (
SELECT
    count(heron.`orders_id`) as `customer_raw_number_of_orders`,
    heron.`customer_id` as `customer_id`,
    min(heron.`orders_date`) as `customer_first_order_date`,
    max(heron.`orders_date`) as `customer_most_recent_order_date`
FROM
    heron as heron
GROUP BY 
    heron.`customer_id`),
monitor as (
SELECT
    coalesce(obsolete.`customer_raw_number_of_orders`,0) as `customer_number_of_orders`,
    obsolete.`customer_id` as `customer_id`
FROM
    obsolete as obsolete
),
python as (
SELECT
    obsolete.`customer_id` as `customer_id`,
    falcon.`customer_first_name` as `customer_first_name`,
    falcon.`customer_last_name` as `customer_last_name`,
    obsolete.`customer_first_order_date` as `customer_first_order_date`,
    obsolete.`customer_most_recent_order_date` as `customer_most_recent_order_date`,
    monitor.`customer_number_of_orders` as `customer_number_of_orders`
FROM
    obsolete as obsolete

LEFT OUTER JOIN falcon on obsolete.`customer_id` = falcon.`customer_id`

LEFT OUTER JOIN monitor on obsolete.`customer_id` = monitor.`customer_id`

)
SELECT
    python.`customer_id`,
    python.`customer_first_name`,
    python.`customer_last_name`,
    python.`customer_first_order_date`,
    python.`customer_most_recent_order_date`,
    python.`customer_number_of_orders`
FROM
    python
