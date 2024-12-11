from dagster import asset
from dagster_duckdb import DuckDBResource


@asset(deps=[])
def scalar_scalar_split_split_2a1b9ef4ddfd30b8a720fd5891233a0df824a60bcbb6553d4acd76e27d402670(
    duck_db: DuckDBResource,
) -> None:
    with duck_db.get_connection() as conn:
        conn.execute(
            """ 
CREATE OR REPLACE TABLE scalar_scalar_split_split_2a1b9ef4ddfd30b8a720fd5891233a0df824a60bcbb6553d4acd76e27d402670 AS

WITH 
quizzical as (
SELECT
    generic_avalues."int_array" as "generic_int_array",
    generic_avalues."scalar" as "generic_scalar"
FROM
    ((
select [1,2,3,4] as int_array, 2 as scalar
)) as generic_avalues),
highfalutin as (
SELECT
    quizzical."generic_scalar" as "generic_scalar",
    unnest(quizzical."generic_int_array") as "generic_split"
FROM
    quizzical),
cheerful as (
SELECT
    highfalutin."generic_scalar" as "cte_generic_scalar",
    highfalutin."generic_scalar" as "generic_scalar",
    highfalutin."generic_split" as "cte_generic_split"
FROM
    highfalutin
WHERE
    highfalutin."generic_split" in (1,2,3)
),
wakeful as (
SELECT
    highfalutin."generic_scalar" as "cte_generic_scalar",
    highfalutin."generic_scalar" as "generic_scalar",
    highfalutin."generic_split" as "cte_generic_split",
    highfalutin."generic_split" as "generic_split"
FROM
    highfalutin
WHERE
    highfalutin."generic_split" in (1,2,3)
),
thoughtful as (
SELECT
    cheerful."cte_generic_scalar" as "cte_generic_scalar",
    quizzical."generic_int_array" as "generic_int_array",
    quizzical."generic_scalar" as "generic_scalar"
FROM
    quizzical
    LEFT OUTER JOIN cheerful on quizzical."generic_scalar" = cheerful."generic_scalar"),
cooperative as (
SELECT
    thoughtful."cte_generic_scalar" as "cte_generic_scalar",
    thoughtful."generic_scalar" as "generic_scalar",
    unnest(thoughtful."generic_int_array") as "generic_split"
FROM
    thoughtful),
questionable as (
SELECT
    cooperative."cte_generic_scalar" as "cte_generic_scalar",
    cooperative."generic_scalar" as "generic_scalar",
    cooperative."generic_split" as "generic_split"
FROM
    cooperative)
SELECT
    questionable."generic_split" as "generic_split",
    questionable."generic_scalar" as "generic_scalar",
    wakeful."cte_generic_split" as "cte_generic_split",
    wakeful."cte_generic_scalar" as "cte_generic_scalar"
FROM
    questionable
    LEFT OUTER JOIN wakeful on questionable."cte_generic_scalar" = wakeful."cte_generic_scalar" AND questionable."generic_scalar" = wakeful."generic_scalar" AND questionable."generic_split" = wakeful."generic_split" """
        )
