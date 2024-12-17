import os
from pathlib import Path as PathlibPath
from sys import path as sys_path

from click import Path, argument, group, option
from trilogy.dialect.enums import Dialects
from trilogy.parsing.render import Renderer

# handles development cases
nb_path = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
sys_path.insert(0, nb_path)

from trilogyt.scripts.dbt import dbt_wrapper, dbt_string_command_wrapper  # noqa
from trilogyt.scripts.native import (  # noqa
    native_wrapper,
    native_string_command_wrapper,
)
from trilogyt.scripts.dagster import (  # noqa
    dagster_wrapper,
    dagster_string_command_wrapper,
)

renderer = Renderer()


@group()
def main():
    """A CLI with three subcommands: dbt, dagster and trilogy"""
    pass


@main.command()
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


@main.command()
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


@main.command()
@argument("preql", type=Path())
@argument("output_path", type=Path(exists=True))
# @argument("write_path", type=Path(exists=True))
@argument("dialect", type=str)
@option("--run", is_flag=True, type=bool, default=False)
@option("--debug", type=bool, default=False)
def dagster(preql: str | Path, output_path: Path, dialect: str, debug: bool, run: bool):

    edialect = Dialects(dialect)
    preqlt: PathlibPath = PathlibPath(str(preql))
    if preqlt.exists():
        return dagster_wrapper(
            preqlt, PathlibPath(str(output_path)), edialect, debug, run
        )
    return dagster_string_command_wrapper(
        str(preql), PathlibPath(str(output_path)), edialect, debug, run
    )


if __name__ == "__main__":
    main()
