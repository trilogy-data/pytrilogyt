import dagster as dg
import pandas as pd
from dagster_duckdb import DuckDBResource

sample_data_file = "src/my_project/defs/data/sample_data.csv"
processed_data_file = "src/my_project/defs/data/processed_data.csv"


@dg.asset
def processed_data():
    ## Read data from the CSV
    df = pd.read_csv(sample_data_file)

    ## Add an age_group column based on the value of age
    df["age_group"] = pd.cut(
        df["age"], bins=[0, 30, 40, 100], labels=["Young", "Middle", "Senior"]
    )

    ## Save processed data
    df.to_csv(processed_data_file, index=False)
    return "Data loaded successfully"


@dg.asset
def dscte_generic_scalar_02e41b09(duck_db: DuckDBResource) -> None:
    with duck_db.get_connection() as conn:
        conn.execute(
            """ 
CREATE OR REPLACE TABLE dscte_generic_scalar_02e41b09 AS

WITH 
quizzical as (
SELECT
    "generic_avalues"."int_array" as "generic_int_array",
    "generic_avalues"."scalar" as "generic_scalar"
FROM
    (
select [1,2,3,4] as int_array, 2 as scalar
) as "generic_avalues"),
highfalutin as (
SELECT
    "quizzical"."generic_scalar" as "generic_scalar",
    unnest("quizzical"."generic_int_array") as "generic_split"
FROM
    "quizzical")
SELECT
    "highfalutin"."generic_scalar" as "cte_generic_scalar",
    "highfalutin"."generic_split" as "cte_generic_split"
FROM
    "highfalutin"
WHERE
    "highfalutin"."generic_split" in (1,2,3)
 """
        )
