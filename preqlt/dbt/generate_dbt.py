from jinja2 import Template
from pathlib import Path
from preql import Executor, Environment
from preql.dialect.enums import Dialects 
from preqlt.constants import logger, PREQLT_NAMESPACE, OPTIMIZATION_NAMESPACE
from preql.core.models import ProcessedQueryPersist, ProcessedQuery, Persist, Import, Datasource, CTE
from preqlt.enums import PreqltMetrics
from preqlt.core import enrich_environment
from preql.parser import parse
from preql.parser import parse_text
from preqlt.dbt.config import DBTConfig
from yaml import safe_load, dump
import os
from preql.core.query_processor import process_query, process_persist
DEFAULT_DESCRIPTION: str = "No description provided"


def generate_model_text(model_name:str, model_type:str, model_sql:str)->str:
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

def add_dependencies(value:str, possible_dependencies:dict[str, Datasource])->str:
    for key, datasource in possible_dependencies.items():
        datasource_name = datasource.name
        value = value.replace(f'{datasource.address.location} as {datasource.address.location}', f"{{{{ ref('{datasource_name}') }}}} as {datasource.address.location}")
    return value

def generate_model(
    preql_body: str,
    preql_path: Path | None,
    dialect: Dialects,
    config: DBTConfig,
    environment: Environment | None = None,
    extra_imports: list[Import] | None = None,
    optimize: bool = True,
):
    logger.info(
        f"Parsing file {preql_path} with dialect {dialect} and base namespace {config.namespace}"
    )

    env:Environment = environment or Environment(
        working_path=preql_path.parent if preql_path else os.getcwd(),
        namespace=config.namespace,
    )
    exec = Executor(dialect=dialect, engine=dialect.default_engine(), environment=env)
    exec.environment = enrich_environment(exec.environment)
    possible_dependencies = {}
    for extra_import in extra_imports or []:
        with open(extra_import.path) as f:
            local_env, persists = parse_text(f.read(), environment=Environment(working_path=Path(extra_import.path).parent))
            # exec.environment.add_import(extra_import.alias, local_env)
            for q in persists:
                if isinstance(q, Persist):
                    processed = process_persist(local_env, q,)
                    exec.environment.add_datasource(processed.datasource)
                    possible_dependencies[processed.datasource.name] = processed.datasource
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
            for cte in query.ctes:
                for source in cte.source.datasources:
                    if not isinstance(source, Datasource):
                        continue
                    if source.identifier in possible_dependencies:
                        source.address.location = f"'{{{{ ref('{source.identifier}_gen_model') }}}}'"
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
            # get our names to label the model
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
                f"-- Generated from preql source: {preql_path}\n-- Do not edit manually\n"
            )
            # set materialization here
            # TODO: make configurable
            f.write("{{ config(materialized='table') }}\n")
            rewritten = add_dependencies(value, possible_dependencies)
            f.write(rewritten)

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
