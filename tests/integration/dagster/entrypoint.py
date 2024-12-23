from assets.customer_four.dim_splits_four_gen_model import dim_splits_four
from assets.customer_one.dim_splits_gen_model import dim_splits
from assets.customer_three.dim_splits_three_gen_model import dim_splits_three
from assets.customer_two.dim_splits_new_gen_model import dim_splits_new
from assets.optimization.ds2a1b9ef4ddfd30b8a720fd5891233a0df824a60bcbb6553d4acd76e27d402670_gen_model import (
    ds2a1b9ef4ddfd30b8a720fd5891233a0df824a60bcbb6553d4acd76e27d402670,
)
from assets.optimization.ds445831a9e9e484d7f90c672304367319328b5fd8d8093cc8a12c9d788f4e4bd7_gen_model import (
    ds445831a9e9e484d7f90c672304367319328b5fd8d8093cc8a12c9d788f4e4bd7,
)
from assets.optimization.dsdd968c4c1215b184ec36e1ed881d193d3e8e2ad062dd6750257f78115dccdfd7_gen_model import (
    dsdd968c4c1215b184ec36e1ed881d193d3e8e2ad062dd6750257f78115dccdfd7,
)
from dagster import Definitions, define_asset_job
from dagster_duckdb import DuckDBResource

all_job = define_asset_job(
    name="all_job",
    selection=[
        ds445831a9e9e484d7f90c672304367319328b5fd8d8093cc8a12c9d788f4e4bd7,
        dsdd968c4c1215b184ec36e1ed881d193d3e8e2ad062dd6750257f78115dccdfd7,
        ds2a1b9ef4ddfd30b8a720fd5891233a0df824a60bcbb6553d4acd76e27d402670,
        dim_splits_four,
        dim_splits,
        dim_splits_three,
        dim_splits_new,
    ],
)


run_ds445831a9e9e484d7f90c672304367319328b5fd8d8093cc8a12c9d788f4e4bd7 = (
    define_asset_job(
        name="run_ds445831a9e9e484d7f90c672304367319328b5fd8d8093cc8a12c9d788f4e4bd7",
        selection=[ds445831a9e9e484d7f90c672304367319328b5fd8d8093cc8a12c9d788f4e4bd7],
    )
)

run_dsdd968c4c1215b184ec36e1ed881d193d3e8e2ad062dd6750257f78115dccdfd7 = (
    define_asset_job(
        name="run_dsdd968c4c1215b184ec36e1ed881d193d3e8e2ad062dd6750257f78115dccdfd7",
        selection=[dsdd968c4c1215b184ec36e1ed881d193d3e8e2ad062dd6750257f78115dccdfd7],
    )
)

run_ds2a1b9ef4ddfd30b8a720fd5891233a0df824a60bcbb6553d4acd76e27d402670 = (
    define_asset_job(
        name="run_ds2a1b9ef4ddfd30b8a720fd5891233a0df824a60bcbb6553d4acd76e27d402670",
        selection=[ds2a1b9ef4ddfd30b8a720fd5891233a0df824a60bcbb6553d4acd76e27d402670],
    )
)

run_dim_splits_four = define_asset_job(
    name="run_dim_splits_four", selection=[dim_splits_four]
)

run_dim_splits = define_asset_job(name="run_dim_splits", selection=[dim_splits])

run_dim_splits_three = define_asset_job(
    name="run_dim_splits_three", selection=[dim_splits_three]
)

run_dim_splits_new = define_asset_job(
    name="run_dim_splits_new", selection=[dim_splits_new]
)


defs = Definitions(
    assets=[
        ds445831a9e9e484d7f90c672304367319328b5fd8d8093cc8a12c9d788f4e4bd7,
        dsdd968c4c1215b184ec36e1ed881d193d3e8e2ad062dd6750257f78115dccdfd7,
        ds2a1b9ef4ddfd30b8a720fd5891233a0df824a60bcbb6553d4acd76e27d402670,
        dim_splits_four,
        dim_splits,
        dim_splits_three,
        dim_splits_new,
    ],
    resources={
        "duck_db": DuckDBResource(
            database="dagster.db", connection_config={"enable_external_access": False}
        )
    },
    jobs=[
        all_job,
        run_ds445831a9e9e484d7f90c672304367319328b5fd8d8093cc8a12c9d788f4e4bd7,
        run_dsdd968c4c1215b184ec36e1ed881d193d3e8e2ad062dd6750257f78115dccdfd7,
        run_ds2a1b9ef4ddfd30b8a720fd5891233a0df824a60bcbb6553d4acd76e27d402670,
        run_dim_splits_four,
        run_dim_splits,
        run_dim_splits_three,
        run_dim_splits_new,
    ],
)
