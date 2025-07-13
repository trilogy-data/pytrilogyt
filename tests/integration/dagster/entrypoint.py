
from dagster import Definitions, define_asset_job
from dagster_duckdb import DuckDBResource

from assets.customer_four.dim_splits_four_gen_model import dim_splits_four
from assets.customer_one.dim_splits_one_gen_model import dim_splits_one
from assets.customer_three.dim_splits_three_gen_model import dim_splits_three
from assets.customer_two.dim_splits_two_gen_model import dim_splits_two

all_job = define_asset_job(name="all_job", selection=[dim_splits_four, dim_splits_one, dim_splits_three, dim_splits_two])


run_dim_splits_four = define_asset_job(name="run_dim_splits_four", selection=[dim_splits_four])

run_dim_splits_one = define_asset_job(name="run_dim_splits_one", selection=[dim_splits_one])

run_dim_splits_three = define_asset_job(name="run_dim_splits_three", selection=[dim_splits_three])

run_dim_splits_two = define_asset_job(name="run_dim_splits_two", selection=[dim_splits_two])



defs = Definitions(
    assets=[dim_splits_four, dim_splits_one, dim_splits_three, dim_splits_two],
    resources={
        "duck_db": DuckDBResource(
            database="dagster.db", connection_config={"enable_external_access": False}
        )
    },
    jobs = [all_job, run_dim_splits_four, run_dim_splits_one, run_dim_splits_three, run_dim_splits_two]
)