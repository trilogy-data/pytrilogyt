from pathlib import Path

import dagster as dg
from dagster import definitions, load_from_defs_folder
from dagster_duckdb import DuckDBResource

# sys.path.insert(0, str('C:\\Users\\ethan\\coding_projects\\pypreql-etl\\my-project'))
# export PYTHONPATH="c:/users/ethan/coding_projects/pypreql-etl/my-project:$PYTHONPATH"
# export PYTHONPATH="c:/users/ethan/coding_projects/pypreql-etl/tests/integration/dagster:$PYTHONPATH"
print(
    Path(__file__).parent.parent.parent,
)


@definitions
def defs():
    return dg.Definitions.merge(
        dg.Definitions(
            resources={
                "duck_db": DuckDBResource(
                    database="my_duckdb_database.duckdb",  # required
                )
            }
        ),
        load_from_defs_folder(
            project_root=Path(__file__).parent.parent.parent,
        ),
    )
