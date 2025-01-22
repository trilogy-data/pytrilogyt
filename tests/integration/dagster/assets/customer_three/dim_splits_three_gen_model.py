from dagster_duckdb import DuckDBResource
from dagster import asset

from assets.optimization.ds2a1b9ef4ddfd30b8a720fd5891233a0df824a60bcbb6553d4acd76e27d402670_gen_model import ds2a1b9ef4ddfd30b8a720fd5891233a0df824a60bcbb6553d4acd76e27d402670


@asset(deps=[ds2a1b9ef4ddfd30b8a720fd5891233a0df824a60bcbb6553d4acd76e27d402670])
def dim_splits_three(duck_db: DuckDBResource) -> None:
    with duck_db.get_connection() as conn:
        conn.execute(
           ''' 
CREATE OR REPLACE TABLE dim_splits_three AS

WITH 
quizzical as (
SELECT
    cast(get_current_timestamp() as datetime) as "_trilogyt__created_at"
),
dynamic as (
SELECT
    ds2a1b9ef4ddfd30b8a720fd5891233a0df824a60bcbb6553d4acd76e27d402670."cte_generic_split" as "cte_generic_split"
FROM
    ds2a1b9ef4ddfd30b8a720fd5891233a0df824a60bcbb6553d4acd76e27d402670
GROUP BY 
    ds2a1b9ef4ddfd30b8a720fd5891233a0df824a60bcbb6553d4acd76e27d402670."cte_generic_split")
SELECT
    dynamic."cte_generic_split" as "cte_generic_split",
    quizzical."_trilogyt__created_at" as "_trilogyt__created_at"
FROM
    dynamic
    FULL JOIN quizzical on 1=1 '''
        )
    