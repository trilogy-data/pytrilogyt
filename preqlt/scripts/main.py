from click import command, Path, argument, option, File
from preql import Executor, Environment  # noqa
from preql.dialect.enums import Dialects  # noqa
from datetime import datetime  # noqa
from pathlib import Path as PathlibPath  # noqa
from preql.hooks.query_debugger import DebuggingHook  # noqa
from preqlt.dbt.generate_dbt import generate_model  # noqa
from preqlt.dbt.run_dbt import run_path  # noqa
from preqlt.dbt.config import DBTConfig  # noqa
from os import path


def print_tabulate(q, tabulate):
    result = q.fetchall()
    print(tabulate(result, headers=q.keys(), tablefmt="psql"))


@command("gen-dbt")
@argument("preql", type=File("r"))
@argument("dbt_path", type=Path(exists=True))
# @argument("write_path", type=Path(exists=True))
@argument("dialect", type=str)
@option("--run", type=bool, default=False)
@option("--debug", type=bool, default=False)
def main(preql: File, dbt_path, dialect: str, debug: bool, run: bool):

    edialect = Dialects(dialect)
    if path.exists(preql.name):
        inputp = PathlibPath(preql.name)
    else:
        inputp = None
    config = DBTConfig(root=PathlibPath(dbt_path), namespace=inputp.stem)
    generate_model(preql.read(), inputp, dialect=edialect, config=config)
    if run:
        run_path(dbt_path)


if __name__ == "__main__":
    main()
