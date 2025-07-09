from dagster import asset
from dagster_duckdb import DuckDBResource

from assets.optimization.dsdd968c4c1215b184ec36e1ed881d193d3e8e2ad062dd6750257f78115dccdfd7_gen_model import (
    dsdd968c4c1215b184ec36e1ed881d193d3e8e2ad062dd6750257f78115dccdfd7,
)


@asset(deps=[dsdd968c4c1215b184ec36e1ed881d193d3e8e2ad062dd6750257f78115dccdfd7])
def dim_splits(duck_db: DuckDBResource) -> None:
    with duck_db.get_connection() as conn:
        conn.execute(
            """ 
CREATE OR REPLACE TABLE dim_splits AS

WITH 
quizzical as (
SELECT
    cast(get_current_timestamp() as datetime) as "_trilogyt__created_at"
)
SELECT
    "dsdd968c4c1215b184ec36e1ed881d193d3e8e2ad062dd6750257f78115dccdfd7"."generic_split" as "generic_split",
    "quizzical"."_trilogyt__created_at" as "_trilogyt__created_at"
FROM
    "dsdd968c4c1215b184ec36e1ed881d193d3e8e2ad062dd6750257f78115dccdfd7"
    FULL JOIN "quizzical" on 1=1 """
        )
