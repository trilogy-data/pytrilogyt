import os
import tempfile
from pathlib import Path as PathlibPath

from trilogy.dialect.enums import Dialects
from trilogy.utility import unique

from trilogyt.constants import OPTIMIZATION_NAMESPACE, logger
from trilogyt.dagster.config import DagsterConfig
from trilogyt.dagster.generate import (
    ModelInput,
    generate_entry_file,
    generate_model,
    generate_name_ds_mapping,
)
from trilogyt.dagster.run import run_path
from trilogyt.scripts.native import native_wrapper


def dagster_handler(
    staging_path: PathlibPath,
    preql: PathlibPath,
    dagster_path: PathlibPath,
    dialect: Dialects,
    debug: bool,
) -> list[ModelInput]:
    logger.info("Optimizing trilogy files...")

    native_wrapper(
        preql=preql,
        output_path=staging_path,
        dialect=dialect,
        debug=debug,
        run=False,
    )
    logger.info("Generating dagster models...")
    logger.info("clearing optimization path")
    opt = dagster_path / "models" / OPTIMIZATION_NAMESPACE

    if opt.exists():
        for item in opt.glob("*.sql"):
            logger.debug(f"Removing existing {item}")
            os.remove(item)
    models: list[ModelInput] = []

    inputs = list(staging_path.glob("*.preql"))
    model_ds_mapping: dict[str, str] = {}
    for file in inputs:
        model_ds_mapping.update(
            generate_name_ds_mapping(file, file.read_text(), dialect)
        )

    for file in inputs:
        logger.info(
            f"Generating dagster model for {file} into dagster_path {dagster_path}"
        )

        optimization = file.stem.startswith("_")
        clear = False
        if optimization:
            config = DagsterConfig(
                root=PathlibPath(dagster_path), namespace=OPTIMIZATION_NAMESPACE
            )
        else:
            clear = True
            config = DagsterConfig(root=PathlibPath(dagster_path), namespace=file.stem)
        with open(file) as f:
            base_models = generate_model(
                f.read(),
                file,
                dialect=dialect,
                config=config,
                clear_target_dir=clear,
                model_ds_mapping=model_ds_mapping,
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
    config = DagsterConfig(root=dagster_path, namespace=preql.stem)
    if preql.is_file():
        with open(preql) as f:
            imports += generate_model(
                f.read(),
                preql,
                dialect=dialect,
                config=config,
                model_ds_mapping={},
                # environment = env  # type: ignore
            )
    else:
        if staging_path:
            imports += dagster_handler(
                staging_path, preql, dagster_path, dialect, debug
            )
        else:
            with tempfile.TemporaryDirectory() as tmpdirname:
                new_path = PathlibPath(tmpdirname)
                imports += dagster_handler(
                    new_path, preql, dagster_path, dialect, debug
                )
    imports = unique(imports, lambda x: x.name)
    for k in imports:
        logger.info(k)
    _ = generate_entry_file(imports, dialect, dagster_path, config)
    if run:
        print(f"Executing generated models in {dagster_path}")
        run_path(PathlibPath(dagster_path), config=config)
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
        model_ds_mapping={},
    )
    _ = generate_entry_file(imports, dialect, dagster_path, config)
    if run:
        print("Executing generated models")
        run_path(PathlibPath(dagster_path), config=config)
    return 0
