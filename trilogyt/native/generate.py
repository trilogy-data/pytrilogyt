import os
from collections import Counter
from pathlib import Path

from trilogy import Environment
from trilogy.core.models import (
    ConceptDeclarationStatement,
    Datasource,
    ImportStatement,
    PersistStatement,
    SelectItem,
)
from trilogy.parser import parse_text
from trilogy.parsing.render import Renderer

from trilogyt.constants import TRILOGY_NAMESPACE, logger
from trilogyt.core import enrich_environment
from trilogyt.enums import PreqltMetrics
from trilogyt.scripts.core import OptimizationResult


def generate_model(
    preql_body: str,
    preql_path: Path,
    output_path: Path,
    optimization: OptimizationResult | None = None,
    environment: Environment | None = None,
):
    logger.info(f"Parsing file {preql_path} with output path {output_path}")

    renderer = Renderer()

    env: Environment = environment or Environment(
        working_path=preql_path.parent if preql_path else os.getcwd(),
        # namespace=config.namespace,
    )

    env = enrich_environment(env)
    possible_dependencies = {}
    persist_override = {}
    if optimization:
        with open(optimization.datasource_path) as f:
            local_env, queries = parse_text(
                f.read(),
                environment=Environment(
                    working_path=Path(optimization.datasource_path).parent
                ),
            )
        datasources = [x for x in queries if isinstance(x, Datasource)]
        logger.info(f"Extra dependencies parsed, have {len(datasources)} datasources.")
        for ds in datasources:
            env.add_datasource(ds)
            possible_dependencies[ds.identifier] = ds
            for oc in ds.output_concepts:
                persist_override[oc.address] = oc

    logger.info(f"Reparsing post optimization for {preql_path}.")
    try:
        _, statements = parse_text(preql_body, env)
    except Exception as e:
        raise SyntaxError(f"Unable to parse {preql_body}" + str(e))

    logger.info(Counter([type(c) for c in statements]))
    for _, v in persist_override.items():
        env.add_concept(v, force=True)

    outputs: list[str] = (
        [
            # f"# import trilogyt concepts \nimport {TRILOGY_NAMESPACE};",
            f"# import shared CTE persists into local namespace \nimport {optimization.datasource_path.stem};"
        ]
        if optimization
        else []
    )
    for idx, query in enumerate(statements):
        # get our names to label the model
        if isinstance(query, PersistStatement):
            query.select.selection.append(
                SelectItem(
                    content=env.concepts[
                        f"{TRILOGY_NAMESPACE}.{PreqltMetrics.CREATED_AT.value}"
                    ]
                )
            )
        last_stmt = statements[idx - 1]
        if last_stmt.__class__ != query.__class__ or not isinstance(
            query, (ImportStatement, ConceptDeclarationStatement)
        ):
            outputs.append("\n")
        outputs.append(renderer.to_string(query))
    logger.info("Writing queries to output files")

    should_exist = set()
    write_path = output_path / f"{preql_path.stem}.preql"
    logger.info("Writing to %s", write_path)
    write_path.parent.mkdir(parents=True, exist_ok=True)
    should_exist.add(write_path)
    with open(write_path, "w") as f:
        f.write(
            f"# Generated from preql source: {preql_path}\n# Do not edit manually\n"
        )
        f.write("\n".join(outputs))
