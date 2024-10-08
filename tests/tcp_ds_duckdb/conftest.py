from trilogy import Dialects, Environment, Executor
from trilogy.dialect.config import DuckDBConfig
import pytest
from trilogy.hooks.query_debugger import DebuggingHook
from pathlib import Path
from logging import INFO

working_path = Path(__file__).parent

SCALE_FACTOR = 5


@pytest.fixture(scope="session")
def engine():
    env = Environment(working_path=working_path)
    engine: Executor = Dialects.DUCK_DB.default_executor(
        environment=env,
        hooks=[DebuggingHook(level=INFO, process_other=False, process_ctes=False)],
        conf=DuckDBConfig(),
    )
    db_path = working_path / f"sf_{SCALE_FACTOR}" / "memory"
    memory = db_path / "schema.sql"

    if Path(memory).exists():
        # TODO: Detect if loaded
        engine.execute_raw_sql(f"IMPORT DATABASE '{db_path}';")
    else:
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    results = engine.execute_raw_sql("SHOW TABLES;").fetchall()
    tables = [r[0] for r in results]

    if "store_sales" not in tables:
        engine.execute_raw_sql(
            f"""
        INSTALL tpcds;
        LOAD tpcds;
        SELECT * FROM dsdgen(sf={SCALE_FACTOR});
        EXPORT DATABASE '{db_path}' (FORMAT PARQUET);"""
        )
    yield engine
