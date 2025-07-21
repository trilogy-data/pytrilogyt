from dagster_duckdb import DuckDBResource
from dagster import asset

from assets.optimization.dsgeneric_scalar_0c9429cc_gen_model import dsgeneric_scalar_0c9429cc

from assets.optimization.dsgeneric_scalar_0c9429cc_gen_model import dsgeneric_scalar_0c9429cc


@asset(deps=[dsgeneric_scalar_0c9429cc, dsgeneric_scalar_0c9429cc])
def dim_splits_one(duck_db: DuckDBResource) -> None:
    with duck_db.get_connection() as conn:
        conn.execute(
           ''' 
CREATE OR REPLACE TABLE dim_splits_one AS
SELECT
    "dsgeneric_scalar_0c9429cc"."generic_split" as "generic_split"
FROM
    "dsgeneric_scalar_0c9429cc"
GROUP BY 
    "dsgeneric_scalar_0c9429cc"."generic_split" '''
        )
    