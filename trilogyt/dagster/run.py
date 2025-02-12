import importlib.util
import os
import subprocess
from pathlib import Path
from typing import Any

from trilogy import Dialects

from trilogyt.constants import logger
from trilogyt.dagster.constants import ALL_JOB_NAME, ENTRYPOINT_FILE


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


def run_in_process(path: Path, imports: list[Path], dialect: Dialects):
    from dagster import materialize

    selection = []
    for file in set(imports):
        logger.info(f"found file: {file}")
        selection.append(import_asset_from_file(file))
    if not selection:
        raise SyntaxError("No assets found in path")
    resources = {}
    if dialect == Dialects.DUCK_DB:
        from dagster_duckdb import DuckDBResource

        resources = {
            "duck_db": DuckDBResource(
                database="dagster.db",  # required
                # tests wll error
                connection_config={"enable_external_access": False},
            )
        }
    else:
        raise NotImplementedError(f"Unsupported dialect: {dialect}")

    result = materialize(assets=selection, resources=resources, selection=selection)
    print(f"Job result: {result}")


def run_dagster_job(path: Path):
    """
    Run a Dagster job using the Dagster CLI.

    Args:
        job_name (str): The name of the Dagster job to execute.
        repository_yaml (str): Path to the `repository.yaml` configuration file.
        workspace_file (str, optional): Path to the Dagster workspace file, if required.

    """
    # config_yaml = path / "repository.yaml"
    # Construct the CLI command
    command = [
        "dagster",
        "job",
        "execute",
        "-j",
        ALL_JOB_NAME,
        # "--config",
        # str(config_yaml),
        "-f",
        ENTRYPOINT_FILE,
    ]

    try:
        # Run the command
        result = subprocess.check_output(command, cwd=str(path))
        print("Dagster executed successfully.\n")
        print("Output:")
        print(result)

    except subprocess.CalledProcessError as e:
        raise ValueError(e.output)


def run_path(path: Path, dialect: Dialects):
    run_dagster_job(path)
