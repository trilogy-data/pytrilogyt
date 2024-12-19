
from dagster import Definitions, define_asset_job
from dagster_duckdb import DuckDBResource

from  import ds445831a9e9e484d7f90c672304367319328b5fd8d8093cc8a12c9d788f4e4bd7_gen_model.py
from  import dsdd968c4c1215b184ec36e1ed881d193d3e8e2ad062dd6750257f78115dccdfd7_gen_model.py
from  import ds2a1b9ef4ddfd30b8a720fd5891233a0df824a60bcbb6553d4acd76e27d402670_gen_model.py
from  import dim_splits_four_gen_model.py
from  import dim_splits_gen_model.py
from  import dim_splits_three_gen_model.py
from  import dim_splits_new_gen_model.py

all_job = define_asset_job(name="all_job", selection=[ds445831a9e9e484d7f90c672304367319328b5fd8d8093cc8a12c9d788f4e4bd7_gen_model.py, dsdd968c4c1215b184ec36e1ed881d193d3e8e2ad062dd6750257f78115dccdfd7_gen_model.py, ds2a1b9ef4ddfd30b8a720fd5891233a0df824a60bcbb6553d4acd76e27d402670_gen_model.py, dim_splits_four_gen_model.py, dim_splits_gen_model.py, dim_splits_three_gen_model.py, dim_splits_new_gen_model.py])


run_ds445831a9e9e484d7f90c672304367319328b5fd8d8093cc8a12c9d788f4e4bd7_gen_model.py = define_asset_job(name="run_ds445831a9e9e484d7f90c672304367319328b5fd8d8093cc8a12c9d788f4e4bd7_gen_model.py", selection=[ds445831a9e9e484d7f90c672304367319328b5fd8d8093cc8a12c9d788f4e4bd7_gen_model.py])

run_dsdd968c4c1215b184ec36e1ed881d193d3e8e2ad062dd6750257f78115dccdfd7_gen_model.py = define_asset_job(name="run_dsdd968c4c1215b184ec36e1ed881d193d3e8e2ad062dd6750257f78115dccdfd7_gen_model.py", selection=[dsdd968c4c1215b184ec36e1ed881d193d3e8e2ad062dd6750257f78115dccdfd7_gen_model.py])

run_ds2a1b9ef4ddfd30b8a720fd5891233a0df824a60bcbb6553d4acd76e27d402670_gen_model.py = define_asset_job(name="run_ds2a1b9ef4ddfd30b8a720fd5891233a0df824a60bcbb6553d4acd76e27d402670_gen_model.py", selection=[ds2a1b9ef4ddfd30b8a720fd5891233a0df824a60bcbb6553d4acd76e27d402670_gen_model.py])

run_dim_splits_four_gen_model.py = define_asset_job(name="run_dim_splits_four_gen_model.py", selection=[dim_splits_four_gen_model.py])

run_dim_splits_gen_model.py = define_asset_job(name="run_dim_splits_gen_model.py", selection=[dim_splits_gen_model.py])

run_dim_splits_three_gen_model.py = define_asset_job(name="run_dim_splits_three_gen_model.py", selection=[dim_splits_three_gen_model.py])

run_dim_splits_new_gen_model.py = define_asset_job(name="run_dim_splits_new_gen_model.py", selection=[dim_splits_new_gen_model.py])



defs = Definitions(
    assets=[ds445831a9e9e484d7f90c672304367319328b5fd8d8093cc8a12c9d788f4e4bd7_gen_model.py, dsdd968c4c1215b184ec36e1ed881d193d3e8e2ad062dd6750257f78115dccdfd7_gen_model.py, ds2a1b9ef4ddfd30b8a720fd5891233a0df824a60bcbb6553d4acd76e27d402670_gen_model.py, dim_splits_four_gen_model.py, dim_splits_gen_model.py, dim_splits_three_gen_model.py, dim_splits_new_gen_model.py],
    resources={
        "": DuckDBResource(
            database="", connection_config={"enable_external_access": False}
        )
    },
    jobs = [all_job, run_ds445831a9e9e484d7f90c672304367319328b5fd8d8093cc8a12c9d788f4e4bd7_gen_model.py, run_dsdd968c4c1215b184ec36e1ed881d193d3e8e2ad062dd6750257f78115dccdfd7_gen_model.py, run_ds2a1b9ef4ddfd30b8a720fd5891233a0df824a60bcbb6553d4acd76e27d402670_gen_model.py, run_dim_splits_four_gen_model.py, run_dim_splits_gen_model.py, run_dim_splits_three_gen_model.py, run_dim_splits_new_gen_model.py]
)