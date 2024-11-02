from trilogy.dialect.enums import Dialects  # noqa
from pathlib import Path as PathlibPath  # noqa
import os
from trilogyt.native.generate import generate_model
from trilogyt.native.run import run_path
from trilogyt.scripts.core import optimize_multiple, OptimizationResult
from trilogyt.constants import logger


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
        with open(preql) as f:
            generate_model(
                f.read(),
                preql,
                output_path=output_path,
                # environment = env  # type: ignore
            )
    else:
        # with multiple files, we can attempt to optimize dependency
        logger.info(f"checking path {preql}")
        children = [
            x for x in list(preql.glob("*.preql")) if not x.stem.startswith("_internal")
        ]
        logger.info(f"optimizing across {children}")
        env_to_optimization = optimize_multiple(
            preql, children, output_path, dialect=dialect
        )
        for file in children:
            with open(file) as f:
                with open(output_path / file.name, "w") as f2:
                    f2.write(f.read())
        for file in children:
            with open(file) as f:
                generate_model(
                    f.read(),
                    file,
                    output_path=output_path,
                    optimization=env_to_optimization.get(file, None),
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
