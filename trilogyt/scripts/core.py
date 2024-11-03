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
    ConceptDeclarationStatement,
    CopyStatement,
    HasUUID,
)
from dataclasses import dataclass
from trilogyt.core import ENVIRONMENT_CONCEPTS, fingerprint_environment

# handles development cases
nb_path = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
sys_path.insert(0, nb_path)

from trilogyt.constants import OPTIMIZATION_NAMESPACE, OPTIMIZATION_FILE  # noqa
from trilogyt.graph import process_raw  # noqa
from trilogyt.exceptions import OptimizationError  # noqa


@dataclass
class OptimizationInput:
    fingerprint: str
    environment: Environment
    statements: list


@dataclass
class OptimizationResult:
    path: PathlibPath
    datasource_path: PathlibPath
    new_import: ImportStatement
    fingerprint: str


renderer = Renderer()


def print_tabulate(q, tabulate):
    result = q.fetchall()
    print(tabulate(result, headers=q.keys(), tablefmt="psql"))


def optimize_multiple(
    base: PathlibPath,
    paths: list[PathlibPath],
    output_path: PathlibPath,
    dialect: Dialects,
) -> dict[PathlibPath, OptimizationResult]:

    optimize_env = Environment(working_path=base.stem, namespace="optimize")
    exec = Executor(
        dialect=dialect, engine=dialect.default_engine(), environment=optimize_env
    )

    env_to_statements: dict[str, OptimizationInput] = {}
    file_to_fingerprint = {}
    for path in paths:
        if path.name.startswith(OPTIMIZATION_FILE):
            continue
        with open(path) as f:
            local_env = Environment(
                working_path=path.parent,
            )
            try:
                new_env, statements = parse_text(f.read(), environment=local_env)
            except Exception as e:
                raise SyntaxError(f"Unable to parse {path} due to {e}")
            if not any(
                isinstance(
                    statement, (SelectStatement, PersistStatement, CopyStatement)
                )
                for statement in statements
            ):
                continue
            fingerprint = fingerprint_environment(new_env)
            file_to_fingerprint[path] = fingerprint
            if fingerprint in env_to_statements:
                opt: OptimizationInput = env_to_statements[fingerprint]
                opt.statements += statements
            else:
                env_to_statements[fingerprint] = OptimizationInput(
                    fingerprint=fingerprint, environment=new_env, statements=statements
                )

    # determine the new persists we need to create
    outputs = {}
    for k, v in env_to_statements.items():
        _, new_persists = process_raw(
            inject=False,
            inputs=v.statements,
            env=v.environment,
            generator=exec.generator,
            threshold=2,
        )

        concept_modifying_statements = unique(
            [x for x in v.statements if isinstance(x, HasUUID)], "uuid"
        )
        final = []
        # we should transform a persist into a select for optimization purposes
        for x in concept_modifying_statements:
            if isinstance(x, PersistStatement):
                final.append(x.select)
            else:
                final.append(x)
        # inject those
        output_file = output_path / f"{OPTIMIZATION_FILE}_{k}.preql"
        with open(output_file, "w") as f:
            for concept in ENVIRONMENT_CONCEPTS:
                f.write(
                    renderer.to_string(ConceptDeclarationStatement(concept=concept))
                    + "\n\n"
                )
            for cte in final:
                f.write(renderer.to_string(cte) + "\n\n")
            for x in new_persists:
                f.write(renderer.to_string(x) + "\n\n")
                # f.write(renderer.to_string(x.datasource) + "\n\n")

        datasource_file = output_path / f"{OPTIMIZATION_FILE}_{k}_datasources.preql"
        with open(datasource_file, "w") as f:
            f.write(f"import {OPTIMIZATION_FILE}_{k};\n\n")
            for x in new_persists:
                f.write(renderer.to_string(x.datasource) + "\n\n")
        outputs[k] = OptimizationResult(
            path=output_file,
            datasource_path=datasource_file,
            new_import=ImportStatement(
                alias=OPTIMIZATION_NAMESPACE,
                path=output_file,
            ),
            fingerprint=k,
        )
    return {k: outputs[v] for k, v in file_to_fingerprint.items()}
