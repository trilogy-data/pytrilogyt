from trilogy.dialect.enums import Dialects  # noqa
from pathlib import Path as PathlibPath  # noqa
from trilogyt.constants import OPTIMIZATION_NAMESPACE  # noqa
from trilogyt.dbt.generate import generate_model  # noqa
from trilogyt.dbt.run import run_path  # noqa
from trilogyt.dbt.config import DBTConfig  # noqa
from trilogyt.scripts.native import native_wrapper, OptimizationResult
from trilogyt.constants import logger
import os
import tempfile


def dbt_handler(
    staging_path: PathlibPath,
    preql: PathlibPath,
    dbt_path: PathlibPath,
    dialect: Dialects,
    debug: bool,
    run: bool,
    children: list[PathlibPath],
):
    logger.info("Optimizing trilogy files...")

    root = (
        native_wrapper(
            preql=preql,
            output_path=staging_path,
            dialect=dialect,
            debug=debug,
            run=False,
        )
        or {}
    )

    logger.info("Generating dbt models...")
    logger.info("clearing optimization path")
    opt = dbt_path / "models" / OPTIMIZATION_NAMESPACE

    if opt.exists():
        for item in opt.glob("*.sql"):
            logger.debug(f"Removing existing {item}")
            os.remove(item)
    for orig_file in children:
        file = staging_path / orig_file.name
        logger.info(f"Generating dbt model for {file} into dbt_path {dbt_path}")
        # don't build hidden files
        if file.stem.startswith("_"):
            continue

        optimization: OptimizationResult | None = root.get(orig_file)

        if optimization:
            with open(optimization.path) as opt_file:
                opt_config = DBTConfig(
                    root=PathlibPath(dbt_path), namespace=OPTIMIZATION_NAMESPACE
                )
                generate_model(
                    opt_file.read(),
                    optimization.path,
                    dialect=dialect,
                    config=opt_config,
                    clear_target_dir=False,
                    # environment = env  # type: ignore
                )
        config = DBTConfig(root=PathlibPath(dbt_path), namespace=file.stem)
        with open(file) as f:
            generate_model(
                f.read(),
                file,
                dialect=dialect,
                config=config,
                # environment = env  # type: ignore
            )


def dbt_wrapper(
    preql: PathlibPath,
    dbt_path: PathlibPath,
    dialect: Dialects,
    debug: bool,
    run: bool,
    staging_path: PathlibPath | None = None,
):
    if preql.is_file():
        config = DBTConfig(root=dbt_path, namespace=preql.stem)
        with open(preql) as f:
            generate_model(
                f.read(),
                preql,
                dialect=dialect,
                config=config,
                # environment = env  # type: ignore
            )
    else:
        children = list(preql.glob("*.preql"))
        if staging_path:
            dbt_handler(staging_path, preql, dbt_path, dialect, debug, run, children)
        else:
            with tempfile.TemporaryDirectory() as tmpdirname:
                new_path = PathlibPath(tmpdirname)
                dbt_handler(new_path, preql, dbt_path, dialect, debug, run, children)

    if run:
        print("Executing generated models")
        run_path(PathlibPath(dbt_path))
    return 0


def dbt_string_command_wrapper(
    preql: str, dbt_path: PathlibPath, dialect: Dialects, debug: bool, run: bool
):
    """handle a string command line input"""
    config = DBTConfig(root=dbt_path, namespace="io")
    generate_model(
        preql,
        dbt_path / "io.preql",
        dialect=dialect,
        config=config,
    )
    if run:
        print("Executing generated models")
        run_path(PathlibPath(dbt_path))
    return 0
