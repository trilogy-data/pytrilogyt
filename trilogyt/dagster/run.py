import os
import importlib.util
from typing import Any
from pathlib import Path
from trilogyt.dagster.constants import SUFFIX
from trilogyt.constants import logger
from trilogy import Dialects
from dagster import materialize
from dagster import define_asset_job, AssetSelection


def import_asset_from_file(filepath: Path) -> Any:
    """
    Dynamically imports an asset from a file based on the filename.

    Args:
        filepath (Path): The path to the Python file containing the asset.
    """
    # Get the file name without extension
    filename = os.path.splitext(os.path.basename(filepath))[0]

    # Load the module from the file path
    spec = importlib.util.spec_from_file_location(filename, filepath)
    if spec is None or spec.loader is None:
        raise ImportError(f"Cannot load module from {filepath}")

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    # Access the asset with the same name as the file
    target_object = filename.rsplit("_", 2)[0]
    if not hasattr(module, target_object):
        raise AttributeError(f"Asset '{target_object}' not found in {filepath}")

    return getattr(module, target_object)


def run_path(path: Path, dialect: Dialects):
    selection = []
    logger.info(f"searching in path: {path}")
    for file in path.glob(f"**/*{SUFFIX}"):
        logger.info(f"found file: {file}")
        selection.append(import_asset_from_file(file))
    if not selection:
        raise SyntaxError("No assets found in path")
    resources = {}
    if dialect == Dialects.DUCK_DB:
        from dagster_duckdb import DuckDBResource

        resources = {
            "duck_db": DuckDBResource(
                database="",  # required
            )
        }
    else:
        raise NotImplementedError(f"Unsupported dialect: {dialect}")

    result = materialize(assets=selection, resources=resources)
    print(f"Job result: {result}")
