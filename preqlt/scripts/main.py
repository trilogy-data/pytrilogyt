from preql.scripts.trilogy import main as preqlmain


from click import command, Path, argument, option
import os
from sys import path

nb_path = os.path.abspath("")
path.insert(0, nb_path)

from preql import Executor, Environment  # noqa
from preql.dialect.enums import Dialects  # noqa
from datetime import datetime  # noqa
from pathlib import Path as PathlibPath  # noqa
from preql.hooks.query_debugger import DebuggingHook  # noqa
from preqlt.dbt.generate_dbt import generate_model
from preqlt.dbt.run_dbt import run_path
from preqlt.dbt.config import DBTConfig

def print_tabulate(q, tabulate):
    result = q.fetchall()
    print(tabulate(result, headers=q.keys(), tablefmt="psql"))


@command('gen-dbt')
@argument("preql_path", type=Path(exists=True))
@argument("dbt_path", type=Path(exists=True))
# @argument("write_path", type=Path(exists=True))
@argument("dialect", type=str)
@option("--debug", type=bool, default=False)
def main(preql_path, dbt_path,  dialect: str, debug: bool):
    edialect = Dialects(dialect)
    inputp = PathlibPath(preql_path)
    config = DBTConfig(root=PathlibPath(dbt_path), namespace=inputp.stem)
    generate_model(inputp,dialect=edialect, config = config)
    run_path(dbt_path)

if __name__ == "__main__":
    main()
