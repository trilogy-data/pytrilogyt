from pathlib import Path
from typing import Union

from click import Path as ClickPath
from click import argument, group, option
from trilogy.dialect.enums import Dialects
from trilogy.parsing.render import Renderer

from trilogyt.scripts.dagster import (
    dagster_string_command_wrapper,
    dagster_wrapper,
)
from trilogyt.scripts.dbt import dbt_string_command_wrapper, dbt_wrapper
from trilogyt.scripts.native import (
    native_string_command_wrapper,
    native_wrapper,
)

renderer = Renderer()


def setup_output_directory(output_path: Union[str, Path]) -> Path:
    """
    Ensure output directory exists and return resolved Path object.
    """
    output = Path(output_path).resolve()
    output.mkdir(exist_ok=True, parents=True)
    return output


def execute_command(
    preql_input: Union[str, Path],
    output_path: Path,
    dialect: Dialects,
    debug: bool,
    run: bool,
    file_wrapper,
    string_wrapper,
):
    """
    Common execution logic for all commands.
    """
    preql_path = Path(preql_input)
    if preql_path.exists():
        return file_wrapper(preql_path.resolve(), output_path, dialect, debug, run)

    # If no file found, treat as string command
    return string_wrapper(str(preql_input), output_path, dialect, debug, run)


@group()
def main():
    """A CLI with three subcommands: dbt, dagster and trilogy"""
    pass


@main.command()
@argument("preql", type=ClickPath())
@argument("output_path", type=ClickPath(exists=False))
@argument("dialect", type=str)
@option("--run", is_flag=True, type=bool, default=False)
@option("--debug", type=bool, default=False)
def dbt(
    preql: Union[str, Path], output_path: Path, dialect: str, debug: bool, run: bool
):
    """DBT command with multiple location checking for preql files."""
    output = setup_output_directory(output_path)
    edialect = Dialects(dialect)

    return execute_command(
        preql,
        output,
        edialect,
        debug,
        run,
        dbt_wrapper,
        dbt_string_command_wrapper,
    )


@main.command()
@argument("preql", type=ClickPath())
@argument("output_path", type=ClickPath(exists=False))
@argument("dialect", type=str)
@option("--run", is_flag=True, type=bool, default=False)
@option("--debug", type=bool, default=False)
def trilogy(
    preql: Union[str, Path], output_path: Path, dialect: str, debug: bool, run: bool
):
    """Trilogy/native command."""
    output = setup_output_directory(output_path)
    edialect = Dialects(dialect)

    return execute_command(
        preql,
        output,
        edialect,
        debug,
        run,
        native_wrapper,
        native_string_command_wrapper,
    )


@main.command()
@argument("preql", type=ClickPath())
@argument("output_path", type=ClickPath(exists=True))  # Note: exists=True for dagster
@argument("dialect", type=str)
@option("--run", is_flag=True, type=bool, default=False)
@option("--debug", type=bool, default=False)
def dagster(
    preql: Union[str, Path], output_path: Path, dialect: str, debug: bool, run: bool
):
    """Dagster command."""
    output = setup_output_directory(output_path)
    edialect = Dialects(dialect)

    return execute_command(
        preql,
        output,
        edialect,
        debug,
        run,
        dagster_wrapper,
        dagster_string_command_wrapper,
    )


if __name__ == "__main__":
    main()
