
from pathlib import Path

import dagster as dg
from dagster_duckdb import DuckDBResource


@dg.definitions
def defs():
    return dg.Definitions.merge(
        dg.Definitions(
            resources={
                "duck_db": DuckDBResource(
                    database="dagster.db", connection_config={"enable_external_access": False}
                )
            }
        ),
        dg.load_from_defs_folder(
            project_root=Path(__file__).parent.parent.parent,
        ),
    )