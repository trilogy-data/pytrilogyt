from click import command, Path, argument, option, File
from preql.dialect.enums import Dialects  # noqa
from pathlib import Path as PathlibPath  # noqa
import os
from sys import path as sys_path
from preql import Environment
# handles development cases
nb_path = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
sys_path.insert(0, nb_path)

from preqlt.dbt.generate_dbt import generate_model  # noqa
from preqlt.dbt.run_dbt import run_path  # noqa
from preqlt.dbt.config import DBTConfig  # noqa



def print_tabulate(q, tabulate):
    result = q.fetchall()
    print(tabulate(result, headers=q.keys(), tablefmt="psql"))


@command("gen-dbt")
@argument("preql", type=Path(path_type = PathlibPath))
@argument("dbt_path", type=Path(exists=True))
# @argument("write_path", type=Path(exists=True))
@argument("dialect", type=str)
@option("--run", is_flag=True, type=bool, default=False)
@option("--debug", type=bool, default=False)
def main(preql: str | Path, dbt_path:Path, dialect: str, debug: bool, run: bool):

    edialect = Dialects(dialect)
    preql:PathlibPath = PathlibPath(preql)
    # env = Environment()
    if preql.exists:
        inputp = preql
        if preql.is_file():
            config = DBTConfig(root=PathlibPath(dbt_path), namespace=preql.stem)
            with open(preql) as f:
                generate_model(
                    f.read(), inputp, dialect=edialect, config=config,
                    # environment = env  # type: ignore
                )
        else:

            for file in preql.glob("*.preql"):
                config = DBTConfig(root=PathlibPath(dbt_path), namespace=file.stem)
                with open(file) as f:
                    generate_model(
                        f.read(), file, dialect=edialect, config=config,
                        # environment = env  # type: ignore
                    )
    else:
        config = DBTConfig(root=PathlibPath(dbt_path), namespace=None)
        generate_model(
            preql.read(), inputp, dialect=edialect, config=config,
            # environment = env  # type: ignore
        )
    if run:
        run_path(dbt_path)


if __name__ == "__main__":
    main()
