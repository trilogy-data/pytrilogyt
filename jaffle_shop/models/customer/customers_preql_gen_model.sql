-- Generated from preql source: C:\Users\ethan\coding_projects\pypreql-etl\jaffle_shop\models\example\customer.preql
--Do not edit manually
{{ config(materialized='table') }}

WITH 
toucan as (
SELECT
    current_datetime() as `_preqlt__created_at`

),
dingo as (
SELECT
    customer_customers_raw.`id` as `customer_id`,
    customer_customers_raw.`first_name` as `customer_first_name`,
    customer_customers_raw.`last_name` as `customer_last_name`
FROM
    dbt-tutorial.jaffle_shop.customers as customer_customers_raw
),
abundant as (
SELECT
    orders_orders_raw.`id` as `orders_id`,
    orders_orders_raw.`user_id` as `customer_id`,
    orders_orders_raw.`order_date` as `orders_date`
FROM
    dbt-tutorial.jaffle_shop.orders as orders_orders_raw
),
pelican as (
SELECT
    abundant.`orders_id` as `orders_id`,
    dingo.`customer_id` as `customer_id`
FROM
    abundant as abundant

FULL JOIN dingo on abundant.`customer_id` = dingo.`customer_id`

),
viper as (
SELECT
    count(pelican.`orders_id`) as `customer_raw_number_of_orders`,
    pelican.`customer_id` as `customer_id`
FROM
    pelican as pelican
GROUP BY 
    pelican.`customer_id`),
ceaseless as (
SELECT
    coalesce(viper.`customer_raw_number_of_orders`,0) as `customer_number_of_orders`,
    viper.`customer_id` as `customer_id`
FROM
    viper as viper
),
brave as (
SELECT
    abundant.`orders_date` as `orders_date`,
    dingo.`customer_id` as `customer_id`
FROM
    abundant as abundant

FULL JOIN dingo on abundant.`customer_id` = dingo.`customer_id`

),
jay as (
SELECT
    min(brave.`orders_date`) as `customer_first_order_date`,
    brave.`customer_id` as `customer_id`,
    max(brave.`orders_date`) as `customer_most_recent_order_date`
FROM
    brave as brave
GROUP BY 
    brave.`customer_id`),
pintail as (
SELECT
    jay.`customer_first_order_date` as `customer_first_order_date`,
    ceaseless.`customer_id` as `customer_id`,
    ceaseless.`customer_number_of_orders` as `customer_number_of_orders`,
    jay.`customer_most_recent_order_date` as `customer_most_recent_order_date`
FROM
    ceaseless as ceaseless

LEFT OUTER JOIN jay on ceaseless.`customer_id` = jay.`customer_id`

),
courageous as (
SELECT
    pintail.`customer_id` as `customer_id`,
    dingo.`customer_first_name` as `customer_first_name`,
    dingo.`customer_last_name` as `customer_last_name`,
    pintail.`customer_first_order_date` as `customer_first_order_date`,
    pintail.`customer_most_recent_order_date` as `customer_most_recent_order_date`,
    pintail.`customer_number_of_orders` as `customer_number_of_orders`,
    toucan.`_preqlt__created_at` as `_preqlt__created_at`
FROM
    ceaseless as ceaseless

LEFT OUTER JOIN pintail on ceaseless.`customer_number_of_orders` = pintail.`customer_number_of_orders` AND ceaseless.`customer_id` = pintail.`customer_id`

LEFT OUTER JOIN dingo on ceaseless.`customer_id` = dingo.`customer_id`

FULL JOIN toucan on 1=1

)
SELECT
    courageous.`customer_id`,
    courageous.`customer_first_name`,
    courageous.`customer_last_name`,
    courageous.`customer_first_order_date`,
    courageous.`customer_most_recent_order_date`,
    courageous.`customer_number_of_orders`,
    courageous.`_preqlt__created_at`
FROM
    courageous
