from contextlib import contextmanager
from typing import Any, Dict

import duckdb
from dagster import ConfigurableResource
from dagster._utils.backoff import backoff
from pydantic import Field
from trilogy import Dialects


class TrilogyResource(ConfigurableResource):
    """Resource for interacting with a DuckDB database.

    Examples:
        .. code-block:: python

            from dagster import Definitions, asset
            from dagster_duckdb import DuckDBResource

            @asset
            def my_table(duckdb: DuckDBResource):
                with duckdb.get_connection() as conn:
                    conn.execute("SELECT * from MY_SCHEMA.MY_TABLE")

            defs = Definitions(
                assets=[my_table],
                resources={"duckdb": DuckDBResource(database="path/to/db.duckdb")}
            )

    """

    dialect: Dialects

    @classmethod
    def _is_dagster_maintained(cls) -> bool:
        return True

    @contextmanager
    def get_connection(self):
        base = self.dialect.default_engine()
        conn = base.connction 
        conn = backoff(
            fn=duckdb.connect,
            retry_on=(RuntimeError, duckdb.IOException),
            kwargs={
                "database": self.database,
                "read_only": False,
                "config": self.connection_config,
            },
            max_retries=10,
        )

        yield conn

        conn.close()
