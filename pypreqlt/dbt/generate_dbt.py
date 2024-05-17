from jinja2 import Template
from pathlib import Path
from preql import Executor, Environment  # noqa
from preql.dialect.enums import Dialects  # noqa
from datetime import datetime  # noqa
from pathlib import Path as PathlibPath  # noqa
from preql.hooks.query_debugger import DebuggingHook  # noqa
from preql.dialect.enums import Dialects  # noqa
from pypreqlt.constants import logger, PREQLT_NAMESPACE
from preql.core.models import ProcessedQueryPersist, ProcessedQuery, Persist
from pypreqlt.enums import PreqltMetrics
from pypreqlt.core import enrich_environment
from preql.parser import parse_text
from pypreqlt.dbt.config import DBTConfig
from yaml import safe_load, dump
import os

DEFAULT_DESCRIPTION: str = "No description provided"


def generate_model_text(model_name, model_type, model_sql):
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
):
    logger.info(
        f"Parsing file {preql_path} with dialect {dialect} and base namespace {config.namespace}"
    )

    env = environment or Environment(
        working_path=preql_path.parent if preql_path else os.getcwd(),
        namespace=config.namespace,
    )
    exec = Executor(dialect=dialect, engine=dialect.default_engine(), environment=env)
    exec.environment = enrich_environment(exec.environment)
    outputs = {}
    output_data = {}
    _, statements = parse_text(preql_body, exec.environment)
    parsed = [z for z in statements if isinstance(z, Persist)]
    for persist in parsed:
        persist.select.selection.append(  # type: ignore
            exec.environment.concepts[
                f"{PREQLT_NAMESPACE}.{PreqltMetrics.CREATED_AT.value}"
            ]
        )
    queries = exec.generator.generate_queries(exec.environment, parsed)
    for _, query in enumerate(queries):
        if isinstance(query, ProcessedQueryPersist):
            base = ProcessedQuery(
                output_columns=query.output_columns,
                ctes=query.ctes,
                base=query.base,
                joins=query.joins,
                grain=query.grain,
                limit=query.limit,
                where_clause=query.where_clause,
                order_by=query.order_by,
            )
            outputs[query.output_to.address.location.split(".")[-1]] = (
                exec.generator.compile_statement(base)
            )
            output_data[query.output_to.address.location.split(".")[-1]] = (
                query.datasource
            )

    for key, value in outputs.items():
        output_path = config.get_model_path(key)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w") as f:
            f.write(
                f"-- Generated from preql source: {preql_path}\n--Do not edit manually\n"
            )
            # set materialization here
            # TODO: make configurable
            f.write("{{ config(materialized='table') }}\n")
            f.write(value)

        # set config file
        config.config_path.parent.mkdir(parents=True, exist_ok=True)
        if config.config_path.exists():
            with open(config.config_path, "r") as f:
                loaded = safe_load(f.read())
        else:
            loaded = {}
        loaded["version"] = 2
        loaded["models"] = loaded.get("models", [])
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
        loaded["models"] = [x for x in loaded["models"] if x["name"] != nobject["name"]]
        loaded["models"].append(nobject)
        with open(config.config_path, "w") as f:
            f.write(dump(loaded))
