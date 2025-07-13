from trilogy.dialect.enums import Dialects  # noqa
from pathlib import Path as PathlibPath  # noqa
import os
from trilogyt.native.generate import generate_model
from trilogyt.native.run import run_path
from trilogyt.scripts.core import optimize_multiple, OptimizationResult
from trilogyt.constants import logger
from trilogyt.core_v2 import Optimizer
from trilogyt.io import FileWorkspace

def native_wrapper(
    preql: PathlibPath,
    output_path: PathlibPath,
    dialect: Dialects,
    debug: bool,
    run: bool,
) -> dict[PathlibPath, OptimizationResult]:
    logger.info(f"Running native wrapper with {preql} and {output_path}")

    existing = output_path.glob("**/*.preql")
    for item in existing:
        logger.debug(f"Removing existing {item}")
        os.remove(item)

    env_to_optimization = {}
    if preql.is_file():
        base = FileWorkspace(working_path=preql.parent, paths=[preql])
    else:
        children = [
            x for x in list(preql.glob("*.preql")) if not x.stem.startswith("_")
        ]
        base = FileWorkspace(working_path=preql, paths=children)

    optimizer = Optimizer(suffix="")

    optimizations = optimizer.paths_to_optimizations(
        workspace=base, dialect=Dialects.DUCK_DB
    )
    output_workspace = FileWorkspace(working_path=output_path, paths=[])
    output_workspace.wipe()

    optimizer.optimizations_to_files(optimizations, base, output_workspace)

    optimizer.rewrite_files_with_optimizations(
        base, optimizations, output_workspace=output_workspace
    )
    if run:
        print("Executing generated models")
        run_path(output_path, dialect=dialect, env_to_optimization=env_to_optimization)
    return env_to_optimization


def native_string_command_wrapper(
    preql: str, output_path: PathlibPath, dialect: Dialects, debug: bool, run: bool
):
    """handle a string command line input"""
    generate_model(
        preql_body=preql,
        # this doesn't exist, but will after we run
        preql_path=output_path / "io.preql",
        output_path=output_path,
        # environment = env  # type: ignore
    )
    if run:
        print("Executing generated models")
        run_path(
            output_path,
            dialect=dialect,
        )
    return 0
