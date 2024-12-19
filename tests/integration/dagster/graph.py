from dagster import Definitions, define_asset_job
from dagster_duckdb import DuckDBResource

from assets.customer_four.dim_splits_four_gen_model import (
    dim_splits_four,
)
from assets.customer_one.dim_splits_gen_model import (
    dim_splits,
)
from assets.customer_three.dim_splits_three_gen_model import (
    dim_splits_three,
)
from assets.customer_two.dim_splits_new_gen_model import (
    dim_splits_new,
)
from assets.io.static_one_gen_model import static_one
from assets.optimization.scalar_split_dd968c4c1215b184ec36e1ed881d193d3e8e2ad062dd6750257f78115dccdfd7_gen_model import (
    scalar_split_dd968c4c1215b184ec36e1ed881d193d3e8e2ad062dd6750257f78115dccdfd7,
)
from assets.optimization.split_4a0c66eaa9bd766e209f4290311d8aaa8f79548618dc80e573bc787a6dd476e4_gen_model import (
    split_4a0c66eaa9bd766e209f4290311d8aaa8f79548618dc80e573bc787a6dd476e4,
)


number_asset_job = define_asset_job(name="run_all", selection=[dim_splits, dim_splits_new, dim_splits_three, dim_splits_four])


defs = Definitions(
    assets=[
        dim_splits,
        dim_splits_new,
        dim_splits_three,
        dim_splits_four,
        static_one,
        scalar_split_dd968c4c1215b184ec36e1ed881d193d3e8e2ad062dd6750257f78115dccdfd7,
        split_4a0c66eaa9bd766e209f4290311d8aaa8f79548618dc80e573bc787a6dd476e4,
    ],
    resources={
        "duck_db": DuckDBResource(
            database="temp.db", connection_config={"enable_external_access": False}
        ),
    },
    jobs = [number_asset_job]
)
