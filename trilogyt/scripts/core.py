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
    ConceptDeclarationStatement,
)
from dataclasses import dataclass
from trilogyt.core import ENVIRONMENT_CONCEPTS

# handles development cases
nb_path = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
sys_path.insert(0, nb_path)

from trilogyt.constants import OPTIMIZATION_NAMESPACE  # noqa
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


def optimize_multiple(
    base: PathlibPath,
    paths: list[PathlibPath],
    output_path: PathlibPath,
    dialect: Dialects,
) -> OptimizationResult:

    optimize_env = Environment(working_path=base.stem, namespace="optimize")
    exec = Executor(
        dialect=dialect, engine=dialect.default_engine(), environment=optimize_env
    )
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

            optimize_env.add_import(path.stem, new_env)

            for nc, ncv in new_env.concepts.items():
                optimize_env.concepts[nc] = ncv
            for nd, ndn in new_env.datasources.items():
                optimize_env.datasources[nd] = ndn
            optimize_env.gen_concept_list_caches()
        all_statements += statements
    # determine the new persists we need to create
    _, new_persists = process_raw(
        inject=False,
        inputs=[
            x
            for x in all_statements
            if isinstance(
                x, (RowsetDerivationStatement, PersistStatement, SelectStatement)
            )
        ],
        env=optimize_env,
        generator=exec.generator,
        threshold=2,
    )
    ctes: list[RowsetDerivationStatement] = unique(
        [x for x in all_statements if isinstance(x, RowsetDerivationStatement)], "name"
    )
    # inject those
    with open(output_path / OPTIMIZATION_FILE, "w") as f:
        for k, nimport in imports.items():
            f.write(renderer.to_string(nimport) + "\n")
        for concept in ENVIRONMENT_CONCEPTS:
            f.write(
                renderer.to_string(ConceptDeclarationStatement(concept=concept)) + "\n"
            )
        for cte in ctes:
            f.write(renderer.to_string(cte) + "\n")
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
