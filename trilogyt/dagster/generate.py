from jinja2 import Template
from pathlib import Path
from trilogy import Executor, Environment
from trilogy.dialect.enums import Dialects
from trilogy.core.models import (
    ProcessedQueryPersist,
    ProcessedQuery,
    PersistStatement,
    Datasource,
    Address,
)
from trilogyt.constants import logger
from trilogyt.core import enrich_environment
from trilogyt.dagster.config import DagsterConfig
from trilogyt.dagster.constants import SUFFIX
from yaml import safe_load, dump
import os
from collections import defaultdict
from collections import Counter
from dataclasses import dataclass

DEFAULT_DESCRIPTION: str = "No description provided"


@dataclass
class ModelInput:
    name: str
    path: Path

    @property
    def import_path(self) -> str:
        base = self.path.stem.replace("/", ".")
        return f"tests.integration.dagster.assets.optimization.{base}"


def generate_model_text(
    model_name: str,
    model_type: str,
    model_sql: str,
    dialect: Dialects,
    dependencies: list[ModelInput],
) -> str:
    template = Template(
        """
from dagster_duckdb import DuckDBResource
from dagster import asset
{% for dep in deps %}
from {{dep.import_path}} import {{dep.name}}
{% endfor %}

@asset(deps=[{% for dep in deps %}{{dep.name}}{% if not loop.last %}, {% endif %}{% endfor %}])
def {{model_name}}({{dialect.name | lower}}: DuckDBResource) -> None:
    with {{dialect.name | lower}}.get_connection() as conn:
        conn.execute(
           ''' {{model_sql}} '''
        )
    """
    )
    return template.render(
        model_name=model_name,
        model_type=model_type,
        model_sql=model_sql,
        dialect=dialect,
        deps = dependencies,
    )


def generate_entry_file(
    models: list[ModelInput], dialect: Dialects, config: DagsterConfig
):
    template = Template(
        """
{% for model in models %}
from {{model.importpath}} import {{model.name}}
{% endfor %}

defs = Definitions(
    assets=[{% for model in models %}{{model.name}}{% if not loop.last %}, {% endif %}{% endfor %}],
    resources={
        "{{dialect.value}}": DuckDBResource(
            database="",  # required
        )
    },
)"""
    )
    return template.render(
        models=models,
        dialect=Dialects,
    )


def generate_model(
    preql_body: str,
    preql_path: Path | None,
    dialect: Dialects,
    config: DagsterConfig,
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
        d.identifier: d for d in executor.environment.datasources.values() if '.' not in d.identifier
    }
    logger.info(Counter([type(c) for c in statements]))
    parsed = [z for z in statements if isinstance(z, PersistStatement)]

    logger.info("generating queries")
    logger.info(f"possible dependencies are {list(possible_dependencies.keys())}")
    logger.info([str(c) for c in executor.environment.materialized_concepts])

    pqueries = executor.generator.generate_queries(executor.environment, parsed)
    logger.info(f"got {len(pqueries)} queries")
    logger.info(Counter([type(c) for c in pqueries]))
    dependency_map = defaultdict(list)
    for _, query in enumerate(pqueries):
        depends_on = []
        if isinstance(query, ProcessedQueryPersist):
            logger.info(f"Starting on {_}")
            target = query.output_to.address.location
            eligible = {k: v for k, v in possible_dependencies.items() if k != target}
            for cte in query.ctes:
                # handle inlined datasources
                logger.info(f"checking cte {cte.name} with {eligible}")
                if cte.base_name_override in eligible:
                    depends_on.append(
                        ModelInput(
                            cte.base_name_override,
                            config.get_asset_path(cte.base_name_override),
                        )
                    )
                for source in cte.source.datasources:
                    logger.info(source.identifier)
                    if not isinstance(source, Datasource):
                        continue

                    if source.identifier in eligible:
                        depends_on.append(
                            ModelInput(
                                source.identifier,
                                config.get_asset_path(source.identifier),
                            )
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
            key = query.output_to.address.location.split(".")[-1]
            outputs[key] = executor.generator.compile_statement(base)
            output_data[key] = query.datasource
            dependency_map[key] = depends_on
    logger.info("Writing queries to output files")
    logger.info(dependency_map)

    existing = defaultdict(set)
    should_exist = defaultdict(set)
    for key, value in outputs.items():

        output_path = config.get_asset_path(key)
        logger.info(f"writing {key} to {output_path} ")
        parent = str(output_path.parent)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        for subf in output_path.parent.iterdir():
            if subf.is_file() and subf.name.endswith(SUFFIX):
                existing[parent].add(subf)
        should_exist[parent].add(output_path)
        with open(output_path, "w") as f:
            f.write(
                generate_model_text(key, "sql", value, dialect=dialect, dependencies=dependency_map.get(key, []))
            )

    config.config_path.parent.mkdir(parents=True, exist_ok=True)
    with open(config.config_path, "w") as f:
        f.write(
            generate_entry_file(
                [ModelInput(name, path) for name, path in should_exist.items()],
                dialect,
                config,
            )
        )

    if clear_target_dir:
        for key, paths in existing.items():
            for path in paths:
                if path not in should_exist[key]:
                    logger.info("Removing old file: %s", path)
                    os.remove(path)
