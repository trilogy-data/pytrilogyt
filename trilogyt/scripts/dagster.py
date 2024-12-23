import os
import tempfile
from pathlib import Path as PathlibPath

from trilogy.dialect.enums import Dialects
from trilogy.utility import unique

from trilogyt.constants import OPTIMIZATION_NAMESPACE, logger
from trilogyt.dagster.config import DagsterConfig
from trilogyt.dagster.generate import ModelInput, generate_entry_file, generate_model
from trilogyt.dagster.run import run_path
from trilogyt.scripts.native import OptimizationResult, native_wrapper


def dagster_handler(
    staging_path: PathlibPath,
    preql: PathlibPath,
    dagster_path: PathlibPath,
    dialect: Dialects,
    debug: bool,
    children: list[PathlibPath],
) -> list[ModelInput]:
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

    logger.info("Generating dagster models...")
    logger.info("clearing optimization path")
    opt = dagster_path / "models" / OPTIMIZATION_NAMESPACE

    if opt.exists():
        for item in opt.glob("*.sql"):
            logger.debug(f"Removing existing {item}")
            os.remove(item)
    models: list[ModelInput] = []
    for orig_file in children:
        file = staging_path / orig_file.name
        logger.info(
            f"Generating dagster model for {file} into dagster_path {dagster_path}"
        )
        # don't build hidden files
        if file.stem.startswith("_"):
            continue

        optimization: OptimizationResult | None = root.get(orig_file)

        if optimization:
            with open(optimization.path) as opt_file:
                opt_config = DagsterConfig(
                    root=PathlibPath(dagster_path), namespace=OPTIMIZATION_NAMESPACE
                )
                optimization_models = generate_model(
                    opt_file.read(),
                    optimization.path,
                    dialect=dialect,
                    config=opt_config,
                    clear_target_dir=False,
                    # environment = env  # type: ignore
                )
                models += optimization_models
        config = DagsterConfig(root=PathlibPath(dagster_path), namespace=file.stem)
        with open(file) as f:
            base_models = generate_model(
                f.read(), file, dialect=dialect, config=config, models=models
            )
            models += base_models
    return models


def dagster_wrapper(
    preql: PathlibPath,
    dagster_path: PathlibPath,
    dialect: Dialects,
    debug: bool,
    run: bool,
    staging_path: PathlibPath | None = None,
):
    imports: list[ModelInput] = []
    if preql.is_file():
        config = DagsterConfig(root=dagster_path, namespace=preql.stem)
        with open(preql) as f:
            imports += generate_model(
                f.read(),
                preql,
                dialect=dialect,
                config=config,
                # environment = env  # type: ignore
            )
    else:
        children = list(preql.glob("*.preql"))
        if staging_path:
            imports += dagster_handler(
                staging_path, preql, dagster_path, dialect, debug, children
            )
        else:
            with tempfile.TemporaryDirectory() as tmpdirname:
                new_path = PathlibPath(tmpdirname)
                imports += dagster_handler(
                    new_path, preql, dagster_path, dialect, debug, children
                )
    imports = unique(imports, lambda x: x.name)
    for k in imports:
        logger.info(k)
    _ = generate_entry_file(imports, dialect, dagster_path)
    if run:
        print("Executing generated models")
        run_path(PathlibPath(dagster_path), dialect=dialect)
    return 0


def dagster_string_command_wrapper(
    preql: str, dagster_path: PathlibPath, dialect: Dialects, debug: bool, run: bool
):
    """handle a string command line input"""
    config = DagsterConfig(root=dagster_path, namespace="io")
    imports = generate_model(
        preql,
        dagster_path / "io.preql",
        dialect=dialect,
        config=config,
    )
    _ = generate_entry_file(imports, dialect, dagster_path)
    if run:
        print("Executing generated models")
        run_path(PathlibPath(dagster_path), dialect=dialect)
    return 0
