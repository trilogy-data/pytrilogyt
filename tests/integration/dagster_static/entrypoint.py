
from dagster import Definitions, define_asset_job
from dagster_duckdb import DuckDBResource

from assets.io.static_one_gen_model import static_one

all_job = define_asset_job(name="all_job", selection=[static_one])


run_static_one = define_asset_job(name="run_static_one", selection=[static_one])



defs = Definitions(
    assets=[static_one],
    resources={
        "duck_db": DuckDBResource(
            database="dagster.db", connection_config={"enable_external_access": False}
        )
    },
    jobs = [all_job, run_static_one]
)