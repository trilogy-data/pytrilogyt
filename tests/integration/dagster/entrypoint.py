
from dagster import Definitions, define_asset_job
from dagster_duckdb import DuckDBResource

from assets.customer_four.dim_splits_four_gen_model import dim_splits_four
from assets.customer_one.dim_splits_one_gen_model import dim_splits_one
from assets.customer_three.dim_splits_three_gen_model import dim_splits_three
from assets.customer_two.dim_splits_two_gen_model import dim_splits_two
from assets.optimization.dsgeneric_scalar_445831a9_gen_model import dsgeneric_scalar_445831a9
from assets.optimization.dsgeneric_scalar_0c9429cc_gen_model import dsgeneric_scalar_0c9429cc
from assets.optimization.dscte_generic_scalar_02e41b09_gen_model import dscte_generic_scalar_02e41b09

all_job = define_asset_job(name="all_job", selection=[dim_splits_four, dim_splits_one, dim_splits_three, dim_splits_two, dsgeneric_scalar_445831a9, dsgeneric_scalar_0c9429cc, dscte_generic_scalar_02e41b09])


run_dim_splits_four = define_asset_job(name="run_dim_splits_four", selection=[dim_splits_four])

run_dim_splits_one = define_asset_job(name="run_dim_splits_one", selection=[dim_splits_one])

run_dim_splits_three = define_asset_job(name="run_dim_splits_three", selection=[dim_splits_three])

run_dim_splits_two = define_asset_job(name="run_dim_splits_two", selection=[dim_splits_two])

run_dsgeneric_scalar_445831a9 = define_asset_job(name="run_dsgeneric_scalar_445831a9", selection=[dsgeneric_scalar_445831a9])

run_dsgeneric_scalar_0c9429cc = define_asset_job(name="run_dsgeneric_scalar_0c9429cc", selection=[dsgeneric_scalar_0c9429cc])

run_dscte_generic_scalar_02e41b09 = define_asset_job(name="run_dscte_generic_scalar_02e41b09", selection=[dscte_generic_scalar_02e41b09])



defs = Definitions(
    assets=[dim_splits_four, dim_splits_one, dim_splits_three, dim_splits_two, dsgeneric_scalar_445831a9, dsgeneric_scalar_0c9429cc, dscte_generic_scalar_02e41b09],
    resources={
        "duck_db": DuckDBResource(
            database="dagster.db", connection_config={"enable_external_access": False}
        )
    },
    jobs = [all_job, run_dim_splits_four, run_dim_splits_one, run_dim_splits_three, run_dim_splits_two, run_dsgeneric_scalar_445831a9, run_dsgeneric_scalar_0c9429cc, run_dscte_generic_scalar_02e41b09]
)