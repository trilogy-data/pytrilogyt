from click import command, Path, argument, option, File
from preql.dialect.enums import Dialects  # noqa
from pathlib import Path as PathlibPath  # noqa
import os
from sys import path as sys_path
from preql import Environment, Executor
from preql.parser import parse_text
from preql.dialect.enums import Dialects 
from preql.parsing.render import Renderer
from preql.core.models import Import

# handles development cases
nb_path = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
sys_path.insert(0, nb_path)

from preqlt.constants import logger, PREQLT_NAMESPACE, OPTIMIZATION_NAMESPACE
from preqlt.dbt.generate_dbt import generate_model  # noqa
from preqlt.dbt.run_dbt import run_path  # noqa
from preqlt.dbt.config import DBTConfig  # noqa
from preqlt.graph import process_raw
from preqlt.exceptions import OptimizationError
from dataclasses import dataclass

@dataclass
class OptimizationResult:
    path:Path
    new_import:Import

renderer = Renderer()

def print_tabulate(q, tabulate):
    result = q.fetchall()
    print(tabulate(result, headers=q.keys(), tablefmt="psql"))

def optimize_multiple(base:Path, paths:list[Path], dialect:Dialects):

    env = Environment(
        working_path=base.stem,
        namespace='optimize'
    )
    exec = Executor(dialect=dialect, engine=dialect.default_engine(), environment=env)
    all_statements = []
    # first - ingest and proecss all statements
    imports:dict[str, Import] = {}
    for path in paths:
        success = True
        with open(path) as f:
            local_env = Environment(working_path = path.parent)
            try:
                new_env, statements = parse_text(f.read(), environment=local_env)
            except Exception as e:
                raise SyntaxError(f'Unable to parse {path} due to {e}')
            for k, v in new_env.imports.items():
                if k in imports:
                    existing = imports[k]
                    if existing.path != v.path:
                        raise OptimizationError('')
                imports[k] = v
        env.add_import(path.stem, new_env)
        all_statements += statements
    # determine the new persists we need to create
    _, new_persists = process_raw(inject=False, inputs=all_statements, env=env, 
                               generator=exec.generator, threshold=2)
    # inject those
    print('len inputs', len(all_statements))
    print('length new'), len(new_persists)
    with open(base / '_internal_cached_intermediates.preql', 'w') as f:
        for k, nimport in imports.items():
            f.write(renderer.to_string(nimport)+'\n')
        for x in new_persists:
            f.write(renderer.to_string(x)+'\n')
    # add our new import to all modules with persists
    return  OptimizationResult(path= base / '_internal_cached_intermediates.preql',
                               new_import = Import(alias =  OPTIMIZATION_NAMESPACE, path=str(base / '_internal_cached_intermediates.preql')))

    



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
            # with multiple files, we can attempt to optimize dependency
            children = list(preql.glob("*.preql"))
            root = optimize_multiple(preql, children, dialect=edialect)
            config = DBTConfig(root=PathlibPath(dbt_path), namespace=OPTIMIZATION_NAMESPACE)
            with open(root.path) as f:
                generate_model(
                    f.read(), root.path, dialect=edialect, config=config,
                    # environment = env  # type: ignore
                )
            for file in children:
                # don't build hidden files
                if file.stem.startswith('_'):
                    continue
                config = DBTConfig(root=PathlibPath(dbt_path), namespace=file.stem)
                with open(file) as f:
                    generate_model(
                        f.read(), file, dialect=edialect, config=config,
                        extra_imports = [root.new_import],
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
