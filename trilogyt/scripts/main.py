from click import command, Path, argument, option
from trilogy.dialect.enums import Dialects  # noqa
from pathlib import Path as PathlibPath  # noqa
import os
from sys import path as sys_path
from trilogy import Environment, Executor
from trilogy.parser import parse_text
from trilogy.parsing.render import Renderer
from trilogy.core.models import ImportStatement, PersistStatement, SelectStatement
from dataclasses import dataclass

# handles development cases
nb_path = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
sys_path.insert(0, nb_path)

from trilogyt.constants import OPTIMIZATION_NAMESPACE  # noqa
from trilogyt.dbt.generate_dbt import generate_model  # noqa
from trilogyt.dbt.run_dbt import run_path  # noqa
from trilogyt.dbt.config import DBTConfig  # noqa
from trilogyt.graph import process_raw  # noqa
from trilogyt.exceptions import OptimizationError  # noqa


OPTIMIZATION_FILE = "_internal_cached_intermediates.preql"


@dataclass
class OptimizationResult:
    path: PathlibPath
    new_import: ImportStatement


renderer = Renderer()


def print_tabulate(q, tabulate):
    result = q.fetchall()
    print(tabulate(result, headers=q.keys(), tablefmt="psql"))


def optimize_multiple(base: PathlibPath, paths: list[PathlibPath], dialect: Dialects):

    env = Environment(working_path=base.stem, namespace="optimize")
    exec = Executor(dialect=dialect, engine=dialect.default_engine(), environment=env)
    all_statements = []
    # first - ingest and proecss all statements
    imports: dict[str, ImportStatement] = {}
    for path in paths:
        if path.name == OPTIMIZATION_FILE:
            continue
        with open(path) as f:
            local_env = Environment(
                working_path=path.parent,
            )
            try:
                new_env, statements = parse_text(f.read(), environment=local_env)
            except Exception as e:
                raise SyntaxError(f"Unable to parse {path} due to {e}")
            for k, v in new_env.imports.items():
                if k in imports:
                    existing = imports[k]
                    if existing.path != v.path:
                        raise OptimizationError("")
                imports[k] = v
        env.add_import(path.stem, new_env)
        all_statements += statements
    # determine the new persists we need to create
    _, new_persists = process_raw(
        inject=False,
        inputs=[
            x
            for x in all_statements
            if isinstance(x, (PersistStatement, SelectStatement))
        ],
        env=env,
        generator=exec.generator,
        threshold=2,
    )
    # inject those
    print("len inputs", len(all_statements))
    print("length new"), len(new_persists)
    with open(base / OPTIMIZATION_FILE, "w") as f:
        for k, nimport in imports.items():
            f.write(renderer.to_string(nimport) + "\n")
        for x in new_persists:
            f.write(renderer.to_string(x) + "\n")
    # add our new import to all modules with persists
    return OptimizationResult(
        path=base / OPTIMIZATION_FILE,
        new_import=ImportStatement(
            alias=OPTIMIZATION_NAMESPACE,
            path=base / OPTIMIZATION_FILE,
        ),
    )


def main_file_wrapper(
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


def main_io_wrapper(
    preql: str, dbt_path: PathlibPath, dialect: Dialects, debug: bool, run: bool
):
    config = DBTConfig(root=dbt_path, namespace="io")
    generate_model(
        preql,
        dbt_path / "io.preql",
        dialect=dialect,
        config=config,
        # environment = env  # type: ignore
    )
    if run:
        run_path(PathlibPath(dbt_path))
    return 0


@command("gen-dbt")
@argument("preql", type=Path())
@argument("dbt_path", type=Path(exists=True))
# @argument("write_path", type=Path(exists=True))
@argument("dialect", type=str)
@option("--run", is_flag=True, type=bool, default=False)
@option("--debug", type=bool, default=False)
def main(preql: str | Path, dbt_path: Path, dialect: str, debug: bool, run: bool):

    edialect = Dialects(dialect)
    preqlt: PathlibPath = PathlibPath(str(preql))
    if preqlt.exists():
        return main_file_wrapper(
            preqlt, PathlibPath(str(dbt_path)), edialect, debug, run
        )
    return main_io_wrapper(str(preql), PathlibPath(str(dbt_path)), edialect, debug, run)


if __name__ == "__main__":
    main()
