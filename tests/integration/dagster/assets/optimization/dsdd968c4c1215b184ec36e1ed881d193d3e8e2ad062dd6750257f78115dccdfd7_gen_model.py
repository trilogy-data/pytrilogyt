from dagster import asset
from dagster_duckdb import DuckDBResource


@asset(deps=[])
def dsdd968c4c1215b184ec36e1ed881d193d3e8e2ad062dd6750257f78115dccdfd7(
    duck_db: DuckDBResource,
) -> None:
    with duck_db.get_connection() as conn:
        conn.execute(
            """ 
CREATE OR REPLACE TABLE dsdd968c4c1215b184ec36e1ed881d193d3e8e2ad062dd6750257f78115dccdfd7 AS

WITH 
quizzical as (
SELECT
    generic_avalues."int_array" as "generic_int_array",
    generic_avalues."scalar" as "generic_scalar"
FROM
    ((
select [1,2,3,4] as int_array, 2 as scalar
)) as generic_avalues)
SELECT
    unnest(quizzical."generic_int_array") as "generic_split",
    quizzical."generic_scalar" as "generic_scalar"
FROM
    quizzical """
        )
