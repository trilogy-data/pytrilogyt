from click import command, Path, argument, option, group
from trilogy.dialect.enums import Dialects  # noqa
from pathlib import Path as PathlibPath  # noqa
import os
from sys import path as sys_path
from trilogy import Environment, Executor
from trilogy.parser import parse_text
from trilogy.parsing.render import Renderer
from trilogy.utility import unique
from trilogy.core.models import (
    ImportStatement,
    PersistStatement,
    SelectStatement,
    RowsetDerivationStatement,
)
from dataclasses import dataclass
from trilogyt.constants import OPTIMIZATION_NAMESPACE  # noqa
from trilogyt.dbt.generate import generate_model  # noqa
from trilogyt.dbt.run import run_path  # noqa
from trilogyt.dbt.config import DBTConfig  # noqa
from trilogyt.graph import process_raw  # noqa
from trilogyt.exceptions import OptimizationError  # noqa
from trilogyt.scripts.core import optimize_multiple

def dbt_wrapper(
    preql: PathlibPath, dbt_path: PathlibPath, dialect: Dialects, debug: bool, run: bool
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
        # with multiple files, we can attempt to optimize dependency
        children = list(preql.glob("*.preql"))
        root = optimize_multiple(preql, children, dialect=dialect)
        config = DBTConfig(root=PathlibPath(dbt_path), namespace=OPTIMIZATION_NAMESPACE)
        with open(root.path) as f:
            generate_model(
                f.read(),
                root.path,
                dialect=dialect,
                config=config,
                # environment = env  # type: ignore
            )
        for file in children:
            # don't build hidden files
            if file.stem.startswith("_"):
                continue
            config = DBTConfig(root=PathlibPath(dbt_path), namespace=file.stem)
            with open(file) as f:
                generate_model(
                    f.read(),
                    file,
                    dialect=dialect,
                    config=config,
                    extra_imports=[root.new_import],
                    # environment = env  # type: ignore
                )
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
        # environment = env  # type: ignore
    )
    if run:
        print("Executing generated models")
        run_path(PathlibPath(dbt_path))
    return 0