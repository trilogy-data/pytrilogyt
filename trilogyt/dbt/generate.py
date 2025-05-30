import os
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path

from jinja2 import Template
from trilogy import Environment, Executor
from trilogy.authoring import (

    Datasource,
    PersistStatement,
)
from trilogy.core.models.datasource import Address
from trilogy.core.models.execute import UnionCTE
from trilogy.core.statements.execute import ProcessedQueryPersist, ProcessedQuery
from trilogy.dialect.enums import Dialects
from yaml import dump, safe_load

from trilogyt.constants import logger
from trilogyt.core import enrich_environment
from trilogyt.dbt.config import DBTConfig

DEFAULT_DESCRIPTION: str = "No description provided"


@dataclass
class QueryProcessingOutput:
    label: str
    sql: str
    datasource: Datasource


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


def handle_processed_query(
    query: ProcessedQueryPersist,
    possible_dependencies: dict[str, Datasource],
    executor: Executor,
) -> QueryProcessingOutput:
    target = query.output_to.address.location
    eligible = {k: v for k, v in possible_dependencies.items() if k != target}
    for cte in query.ctes:
        # handle inlined datasources
        logger.info(f"checking cte {cte.name} with {eligible}")
        if isinstance(cte, UnionCTE):
            continue
        if cte.base_name_override in eligible:
            cte.base_name_override = (
                f"{{{{ ref('{cte.base_name_override}_gen_model') }}}}"
            )
        for source in cte.source.datasources:
            logger.info(source.identifier)
            if not isinstance(source, Datasource):
                continue

            if source.identifier in eligible:
                if isinstance(source.address, Address):
                    source.address.location = (
                        f"{{{{ ref('{source.identifier}_gen_model') }}}}"
                    )
                elif isinstance(source.address, str):
                    source.address = f"{{{{ ref('{source.identifier}_gen_model') }}}}"
    base = ProcessedQuery(
        output_columns=query.output_columns,
        ctes=query.ctes,
        base=query.base,
        hidden_columns=query.hidden_columns,
        limit=query.limit,
        order_by=query.order_by,
    )
    return QueryProcessingOutput(
        label=query.output_to.address.location.split(".")[-1],
        sql=executor.generator.compile_statement(base),
        datasource=query.datasource,
    )


def generate_model(
    preql_body: str,
    preql_path: Path | None,
    dialect: Dialects,
    config: DBTConfig,
    environment: Environment | None = None,
    clear_target_dir: bool = True,
):
    env: Environment = environment or Environment(
        working_path=preql_path.parent if preql_path else os.getcwd(),
        # namespace=config.namespace,
    )
    executor = Executor(
        dialect=dialect, engine=dialect.default_engine(), environment=env
    )
    executor.environment = enrich_environment(executor.environment)
    possible_dependencies = {}

    outputs: dict[str, str] = {}
    output_data: dict[str, Datasource] = {}
    try:
        _, statements = executor.environment.parse(preql_body)
    except Exception as e:
        raise SyntaxError(f"Unable to parse {preql_body}" + str(e))

    possible_dependencies = {
        d.identifier: d for d in executor.environment.datasources.values()
    }
    logger.info(Counter([type(c) for c in statements]))
    parsed = [z for z in statements if isinstance(z, PersistStatement)]

    logger.info("generating queries")
    logger.info(f"possible dependencies are {list(possible_dependencies.keys())}")
    logger.info([str(c) for c in executor.environment.materialized_concepts])

    pqueries = executor.generator.generate_queries(executor.environment, parsed)
    logger.info(f"got {len(pqueries)} queries")
    logger.info(Counter([type(c) for c in pqueries]))
    for _, query in enumerate(pqueries):
        if isinstance(query, ProcessedQueryPersist):
            parsed_query = handle_processed_query(
                query, possible_dependencies, executor
            )
            outputs[parsed_query.label] = parsed_query.sql
            output_data[parsed_query.label] = parsed_query.datasource

    logger.info("Writing queries to output files")
    existing = defaultdict(set)
    should_exist = defaultdict(set)
    for key, value in outputs.items():

        output_path = config.get_model_path(key)
        logger.info(f"writing {key} to {output_path} ")
        parent = str(output_path.parent)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        for subf in output_path.parent.iterdir():
            if subf.is_file() and subf.name.endswith("gen_model.sql"):
                existing[parent].add(subf)
        should_exist[parent].add(output_path)
        with open(output_path, "w") as f:
            if preql_path:
                f.write(
                    f"-- Generated from preql source: {preql_path.stem}\n-- Do not edit manually\n"
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
            "description": "Automatically generated model from trilogy",
            "columns": columns,
        }
        # loaded["models"] = [x for x in loaded["models"] if x["name"] != nobject["name"]]
        models.append(nobject)

    with open(config.config_path, "w") as f:
        loaded["models"] = models
        f.write(dump(loaded))

    if clear_target_dir:
        for key, paths in existing.items():
            for path in paths:
                if path not in should_exist[key]:
                    logger.info("Removing old file: %s", path)
                    os.remove(path)
