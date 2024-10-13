from jinja2 import Template
from pathlib import Path
from trilogy import Executor, Environment
from trilogy.dialect.enums import Dialects
from trilogy.core.models import (
    ProcessedQueryPersist,
    ProcessedQuery,
    PersistStatement,
    ImportStatement,
    Datasource,
    Address,
    SelectItem,
    Concept,
    ConceptTransform,
)
from trilogyt.constants import logger, TRILOGY_NAMESPACE
from trilogyt.enums import PreqltMetrics
from trilogyt.core import enrich_environment
from trilogy.parser import parse_text
from trilogyt.dbt.config import DBTConfig
from yaml import safe_load, dump
import os
from trilogy.core.query_processor import process_persist
from collections import defaultdict
from collections import Counter

DEFAULT_DESCRIPTION: str = "No description provided"


def generate_model_text(model_name: str, model_type: str, model_sql: str) -> str:
    template = Template(
        """
    {{ model_type }} "{{ model_name }}" {
        {{ model_sql }}
    }
    """
    )
    return template.render(
        model_name=model_name, model_type=model_type, model_sql=model_sql
    )


def generate_model(
    preql_body: str,
    preql_path: Path | None,
    dialect: Dialects,
    config: DBTConfig,
    environment: Environment | None = None,
    extra_imports: list[ImportStatement] | None = None,
    optimize: bool = True,
):
    logger.info(
        f"Parsing file {preql_path} with dialect {dialect} and base namespace {config.namespace}"
    )

    env: Environment = environment or Environment(
        working_path=preql_path.parent if preql_path else os.getcwd(),
        # namespace=config.namespace,
    )
    executor = Executor(
        dialect=dialect, engine=dialect.default_engine(), environment=env
    )
    executor.environment = enrich_environment(executor.environment)
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
        # exec.environment.add_import(extra_import.alias, local_env)
        for q in persists:
            if isinstance(q, PersistStatement):
                processed = process_persist(
                    local_env,
                    q,
                )
                executor.environment.add_datasource(processed.datasource)
                possible_dependencies[processed.datasource.identifier] = (
                    processed.datasource
                )
                for oc in processed.datasource.output_concepts:
                    persist_override[oc.address] = oc

    outputs: dict[str, str] = {}
    output_data: dict[str, Datasource] = {}
    logger.info(f"Reparsing post optimization for {preql_path}.")
    try:
        _, statements = parse_text(preql_body, executor.environment)
    except Exception as e:
        raise SyntaxError(f"Unable to parse {preql_body}" + str(e))

    logger.info(Counter([type(c) for c in statements]))
    for k, v in persist_override.items():
        executor.environment.add_concept(v, force=True)
    parsed = [z for z in statements if isinstance(z, PersistStatement)]
    for persist in parsed:
        persist.select.selection.append(
            SelectItem(
                content=executor.environment.concepts[
                    f"{TRILOGY_NAMESPACE}.{PreqltMetrics.CREATED_AT.value}"
                ]
            )
        )
    logger.info("generating queries")
    logger.info(f"possible dependencies are {list(possible_dependencies.keys())}")
    logger.info([str(c) for c in executor.environment.materialized_concepts])
    for ds in possible_dependencies.values():
        for c in ds.output_concepts:
            assert c.address in executor.environment.materialized_concepts
    pqueries = executor.generator.generate_queries(executor.environment, parsed)
    logger.info(f"got {len(pqueries)} queries")
    logger.info(Counter([type(c) for c in pqueries]))
    for _, query in enumerate(pqueries):
        if isinstance(query, ProcessedQueryPersist):
            logger.info(f"Starting on {_}")
            for cte in query.ctes:
                # handle inlined datasources
                logger.info(f"checking cte {cte.name} with {possible_dependencies}")
                if cte.base_name_override in possible_dependencies:
                    cte.base_name_override = (
                        f"{{{{ ref('{cte.base_name_override}_gen_model') }}}}"
                    )
                for source in cte.source.datasources:
                    logger.info(source.identifier)
                    if not isinstance(source, Datasource):
                        continue
                    if source.identifier in possible_dependencies:
                        if isinstance(source.address, Address):
                            source.address.location = (
                                f"{{{{ ref('{source.identifier}_gen_model') }}}}"
                            )
                        elif isinstance(source.address, str):
                            source.address = (
                                f"{{{{ ref('{source.identifier}_gen_model') }}}}"
                            )
            base = ProcessedQuery(
                output_columns=query.output_columns,
                ctes=query.ctes,
                base=query.base,
                joins=query.joins,
                grain=query.grain,
                hidden_columns=query.hidden_columns,
                limit=query.limit,
                where_clause=query.where_clause,
                order_by=query.order_by,
            )
            # get our names to label the model
            outputs[query.output_to.address.location.split(".")[-1]] = (
                executor.generator.compile_statement(base)
            )
            output_data[query.output_to.address.location.split(".")[-1]] = (
                query.datasource
            )
    logger.info("Writing queries to output files")

    existing = defaultdict(set)
    should_exist = defaultdict(set)
    for key, value in outputs.items():
        output_path = config.get_model_path(key)
        parent = str(output_path.parent)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        for subf in output_path.parent.iterdir():
            if subf.is_file() and subf.name.endswith("gen_model.sql"):
                existing[parent].add(subf)
        should_exist[parent].add(output_path)
        with open(output_path, "w") as f:
            f.write(
                f"-- Generated from preql source: {preql_path}\n-- Do not edit manually\n"
            )
            # set materialization here
            # TODO: make configurable
            f.write("{{ config(materialized='table') }}\n")
            f.write(value)

    config.config_path.parent.mkdir(parents=True, exist_ok=True)
    if config.config_path.exists():
        with open(config.config_path, "r") as f:
            loaded = safe_load(f.read())
    else:
        loaded = {}
    loaded["version"] = 2
    loaded["models"] = loaded.get("models", [])
    models = []
    for key, value in outputs.items():
        ds = output_data[key]
        columns = [
            {
                "name": c.alias,
                "description": (
                    c.concept.metadata.description or DEFAULT_DESCRIPTION
                    if c.concept.metadata
                    else DEFAULT_DESCRIPTION
                ),
                "tests": ["unique"] if [c] == ds.grain.components else [],
            }
            for c in ds.columns
        ]
        nobject = {
            "name": f"{key}_gen_model",
            "description": "Automatically generated model from preql",
            "columns": columns,
        }
        # loaded["models"] = [x for x in loaded["models"] if x["name"] != nobject["name"]]
        models.append(nobject)

    with open(config.config_path, "w") as f:
        loaded["models"] = models
        f.write(dump(loaded))

    for key, paths in existing.items():
        for path in paths:
            if path not in should_exist[key]:
                logger.info("Removing old file: %s", path)
                os.remove(path)
