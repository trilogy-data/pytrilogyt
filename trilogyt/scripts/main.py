from click import Path, argument, option, group
from trilogy.dialect.enums import Dialects
from pathlib import Path as PathlibPath
import os
from sys import path as sys_path
from trilogy.parsing.render import Renderer
from trilogy.core.models import (
    ImportStatement,
)
from dataclasses import dataclass

# handles development cases
nb_path = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
sys_path.insert(0, nb_path)

from trilogyt.scripts.dbt import dbt_wrapper, dbt_string_command_wrapper  # noqa
from trilogyt.scripts.native import (  # noqa
    native_wrapper,
    native_string_command_wrapper,
)

OPTIMIZATION_FILE = "_internal_cached_intermediates.preql"


@dataclass
class OptimizationResult:
    path: PathlibPath
    new_import: ImportStatement


renderer = Renderer()


@group()
def cli():
    """A CLI with two subcommands: dbt and trilogy"""
    pass


@cli.command()
@argument("preql", type=Path())
@argument("output_path", type=Path(exists=True))
# @argument("write_path", type=Path(exists=True))
@argument("dialect", type=str)
@option("--run", is_flag=True, type=bool, default=False)
@option("--debug", type=bool, default=False)
def dbt(preql: str | Path, output_path: Path, dialect: str, debug: bool, run: bool):

    edialect = Dialects(dialect)
    preqlt: PathlibPath = PathlibPath(str(preql))
    if preqlt.exists():
        return dbt_wrapper(preqlt, PathlibPath(str(output_path)), edialect, debug, run)
    return dbt_string_command_wrapper(
        str(preql), PathlibPath(str(output_path)), edialect, debug, run
    )


@cli.command()
@argument("preql", type=Path())
@argument("output_path", type=Path(exists=True))
# @argument("write_path", type=Path(exists=True))
@argument("dialect", type=str)
@option("--run", is_flag=True, type=bool, default=False)
@option("--debug", type=bool, default=False)
def trilogy(preql: str | Path, output_path: Path, dialect: str, debug: bool, run: bool):

    edialect = Dialects(dialect)
    preqlt: PathlibPath = PathlibPath(str(preql))
    if preqlt.exists():
        return native_wrapper(
            preqlt, PathlibPath(str(output_path)), edialect, debug, run
        )
    return native_string_command_wrapper(
        str(preql), PathlibPath(str(output_path)), edialect, debug, run
    )


if __name__ == "__main__":
    cli()
