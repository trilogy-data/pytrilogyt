# add path to trilogyt to PYTHONPATH

import sys
import pathlib
current = pathlib.Path(__file__).parent.parent.parent.resolve()

if str(current) not in sys.path:
    sys.path.append(str(current))

from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv
from trilogy import Dialects, Environment, Executor
from trilogyt.core_v2 import Optimizer
from trilogy.dialect.config import DuckDBConfig
from trilogy.hooks.query_debugger import DebuggingHook
from logging import INFO

SCALE_FACTOR = 0.2

load_dotenv()

working_path = Path(__file__).parent


def run_setup():
    env = Environment(working_path=working_path)
    engine: Executor = Dialects.DUCK_DB.default_executor(
        environment=env,
        # hooks=[DebuggingHook(level=INFO, process_other=False, process_ctes=False)],
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

    root = Path(__file__).parent
    print('db built, starting optimization')
    start = datetime.now()
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

    print(f"Have {len(sources)} files {datetime.now() - start}")

    optimizations = optimizer.paths_to_optimizations(
        working_path=root, files=sources, dialect=Dialects.DUCK_DB
    )
    print(f"Have {len(optimizations)} optimizations {datetime.now() - start}")
    optimizer.wipe_directory(output_path)

    optimizer.optimizations_to_files(optimizations, root, output_path)

    optimizer.rewrite_files_with_optimizations(
        root, sources, optimizations, "_optimized", output_path
    )
    print(f"Rewrote files {datetime.now() - start}")

    optimized_files = list(output_path.glob("**/_trilogyt_opt*.preql"))
    # for file in optimized_files:
    #     print(f"Executing optimized file: {file}")
    #     try:
    #         engine.execute_file(file, non_interactive=True)
    #     except Exception as e:
    #         print(f"Error executing optimized file {file}: {e}")


if __name__ == "__main__":
    run_setup()