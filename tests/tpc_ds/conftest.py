from dataclasses import dataclass
from logging import INFO
from pathlib import Path

import pytest
from trilogy import Dialects, Environment, Executor
from trilogy.dialect.config import DuckDBConfig
from trilogy.hooks.query_debugger import DebuggingHook

from trilogyt.constants import logger
from trilogyt.core_v2 import Optimizer
from trilogyt.scripts.native import OptimizationResult

working_path = Path(__file__).parent

SCALE_FACTOR = 0.5


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


@dataclass
class OptimizedEnv:
    temp_dir: Path
    mappings: dict[Path, OptimizationResult]


@pytest.fixture(scope="session")
def optimized_env(engine: Executor):

    root = Path(__file__).parent
    logger.info(root)
    files = root.glob("*.preql")
    output_path = root / "output"
    if not output_path.exists():
        output_path.mkdir(parents=True, exist_ok=True)
    sources = [
        x
        for x in files
        if not x.stem.startswith("_") and not x.stem.endswith("_optimized")
    ]
    optimizer = Optimizer()

    logger.info(f"Have {len(sources)} files")

    optimizations = optimizer.paths_to_optimizations(
        working_path=root, files=sources, dialect=Dialects.DUCK_DB
    )

    optimizer.wipe_directory(output_path)
    optimizer.optimizations_to_files(optimizations, root, output_path)

    optimizer.rewrite_files_with_optimizations(
        root, sources, optimizations, "_optimized", output_path
    )

    optimized_files = list(output_path.glob("**/_trilogyt_opt*.preql"))
    for file in optimized_files:
        logger.info(f"Executing optimized file: {file}")
        try:
            engine.execute_file(file, non_interactive=True)
        except Exception as e:
            logger.error(f"Error executing optimized file {file}: {e}")
    yield output_path
