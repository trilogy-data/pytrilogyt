from pathlib import Path
from trilogy import Environment
from trilogy.core.models import (
    PersistStatement,
    ImportStatement,
    SelectItem,
    Concept,
    ConceptTransform,
)
from trilogyt.constants import logger, TRILOGY_NAMESPACE
from trilogyt.enums import PreqltMetrics
from trilogyt.core import enrich_environment
from trilogy.parser import parse_text
import os
from trilogy.core.query_processor import process_persist
from collections import Counter
from trilogy.parsing.render import Renderer


def generate_model(
    preql_body: str,
    preql_path: Path,
    output_path: Path,
    environment: Environment | None = None,
    extra_imports: list[ImportStatement] | None = None,
    optimize: bool = True,
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
    for extra_import in extra_imports or []:
        with open(extra_import.path) as f:
            local_env, queries = parse_text(
                f.read(),
                environment=Environment(working_path=Path(extra_import.path).parent),
            )
        persists = [x for x in queries if isinstance(x, PersistStatement)]
        output_cs: list[Concept] = []
        for persist in persists:
            for x in persist.select.selection:
                if isinstance(x.content, ConceptTransform):
                    output_cs.append(x.content.output)
                else:
                    output_cs.append(x.content)
        logger.info(
            f"Extra dependencies parsed, have {len(persists)} persists adding {[x.address for x in output_cs]}."
        )
        for q in persists:
            if isinstance(q, PersistStatement):
                processed = process_persist(
                    local_env,
                    q,
                )
                env.add_datasource(processed.datasource)
                possible_dependencies[processed.datasource.identifier] = (
                    processed.datasource
                )
                for oc in processed.datasource.output_concepts:
                    persist_override[oc.address] = oc

    logger.info(f"Reparsing post optimization for {preql_path}.")
    try:
        _, statements = parse_text(preql_body, env)
    except Exception as e:
        raise SyntaxError(f"Unable to parse {preql_body}" + str(e))

    logger.info(Counter([type(c) for c in statements]))
    for _, v in persist_override.items():
        env.add_concept(v, force=True)

    outputs: list[str] = [
        "# import shared CTE persists into local namespace \nimport _internal_cached_intermediates;"
    ]
    for _, query in enumerate(statements):
        # get our names to label the model
        if isinstance(query, PersistStatement):
            query.select.selection.append(
                SelectItem(
                    content=env.concepts[
                        f"{TRILOGY_NAMESPACE}.{PreqltMetrics.CREATED_AT.value}"
                    ]
                )
            )
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
        f.write("\n\n".join(outputs))
