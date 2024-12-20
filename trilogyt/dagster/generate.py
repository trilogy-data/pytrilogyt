import os
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path

from jinja2 import Template
from trilogy import Environment, Executor
from trilogy.core.models import (
    Datasource,
    PersistStatement,
    ProcessedQueryPersist,
    UnionCTE,
)
from trilogy.dialect.enums import Dialects

from trilogyt.constants import logger
from trilogyt.core import enrich_environment
from trilogyt.dagster.config import DagsterConfig
from trilogyt.dagster.constants import SUFFIX, ALL_JOB_NAME, ENTRYPOINT_FILE

DEFAULT_DESCRIPTION: str = "No description provided"


@dataclass
class ModelInput:
    name: str
    file_path: Path
    import_path: Path

    @property
    def python_import(self) -> str:
        return '.'.join(self.import_path.with_suffix('').parts)


def generate_model_text(
    model_name: str,
    model_type: str,
    model_sql: str,
    dialect: Dialects,
    dependencies: list[ModelInput],
) -> str:
    template = Template(
        """from dagster_duckdb import DuckDBResource
from dagster import asset
{% for dep in deps %}
from {{dep.python_import}} import {{dep.name}}
{% endfor %}

@asset(deps=[{% for dep in deps %}{{dep.name}}{% if not loop.last %}, {% endif %}{% endfor %}])
def {{model_name}}({{dialect.name | lower}}: DuckDBResource) -> None:
    with {{dialect.name | lower}}.get_connection() as conn:
        conn.execute(
           """ {{model_sql}} """
        )
    """
    )
    return template.render(
        model_name=model_name,
        model_type=model_type,
        model_sql=model_sql,
        dialect=dialect,
        deps=dependencies,
    )


def generate_entry_file(
    models: list[ModelInput],
    dialect: Dialects,  # config: DagsterConfig
    dagster_path: Path,
):
    extra_kwargs = {}
    if dialect != Dialects.DUCK_DB:
        raise NotImplementedError(f"Unsupported dialect {dialect}")
    
    if dialect == Dialects.DUCK_DB:
        extra_kwargs["database"] = "dagster.db"
        extra_kwargs["connection_config"] = {"enable_external_access": False}
    template = Template(
        """
from dagster import Definitions, define_asset_job
from dagster_duckdb import DuckDBResource
{% for model in models %}
from {{model.python_import}} import {{model.name}}{% endfor %}

{{all_job_name}} = define_asset_job(name="{{all_job_name}}", selection=[{% for model in models %}{{model.name}}{% if not loop.last %}, {% endif %}{% endfor %}])

{% for model in models %}
run_{{model.name}} = define_asset_job(name="run_{{model.name}}", selection=[{{model.name}}])
{% endfor %}


defs = Definitions(
    assets=[{% for model in models %}{{model.name}}{% if not loop.last %}, {% endif %}{% endfor %}],
    resources={
        "{{dialect.value}}": DuckDBResource(
            database="{{extra_kwargs["database"]}}", connection_config={"enable_external_access": False}
        )
    },
    jobs = [{{all_job_name}}{% for model in models %}, run_{{model.name}}{% endfor %}]
)"""
    )
    contents = template.render(
        models=models,
        dialect=dialect,
        all_job_name = ALL_JOB_NAME,
        extra_kwargs=extra_kwargs,
    )

    with open(dagster_path / ENTRYPOINT_FILE, "w") as f:
        f.write(contents)


def generate_model(
    preql_body: str,
    preql_path: Path | None,
    dialect: Dialects,
    config: DagsterConfig,
    environment: Environment | None = None,
    clear_target_dir: bool = True,
    models: list[ModelInput] = [],
) -> list[ModelInput]:
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
        d.identifier: d
        for d in executor.environment.datasources.values()
        if "." not in d.identifier
    }
    parsed = [z for z in statements if isinstance(z, PersistStatement)]

    logger.info("generating queries")
    logger.info(f"possible dependencies are {list(possible_dependencies.keys())}")

    pqueries = executor.generator.generate_queries(executor.environment, parsed)
    logger.info(f"got {len(pqueries)} queries: {Counter([type(c) for c in pqueries])}")
    dependency_map = defaultdict(list)
    output:list[ModelInput] = []
    for _, query in enumerate(pqueries):
        depends_on: list[ModelInput] = []
        if isinstance(query, ProcessedQueryPersist):
            logger.debug(f"Starting on {_}")
            target = query.output_to.address.location
            eligible = {k: v for k, v in possible_dependencies.items() if k != target}
            for cte in query.ctes:
                if isinstance(cte, UnionCTE):
                    continue
                # handle inlined datasources
                logger.debug(f"checking cte {cte.name} with {eligible}")
                if cte.base_name_override in eligible:
                    if any(x.name == cte.base_name_override for x in depends_on):
                        continue
                    matched = [x for x in models if x.name == cte.base_name_override]
                    if matched:
                        depends_on.append( matched.pop())
                for source in cte.source.datasources:
                    logger.info(source.identifier)
                    if not isinstance(source, Datasource):
                        continue

                    if source.identifier in eligible:
                        matched = [x for x in models if x.name == source.identifier]
                        if matched:
                            depends_on.append( matched.pop())
            # get our names to label the model
            key = query.output_to.address.location.split(".")[-1]
            outputs[key] = executor.generator.compile_statement(query)
            output_data[key] = query.datasource
            dependency_map[key] = depends_on
            output.append(
                ModelInput(
                    name=key,
                    file_path=config.get_asset_path(key),
                    import_path=config.get_asset_import_path(key),
                )
            )
    logger.info("Writing queries to output files")
    logger.debug(dependency_map)

    existing = defaultdict(set)
    should_exist = defaultdict(set)
    for key, value in outputs.items():
        output_path = config.get_asset_path(key)
        logger.info(f"writing {key} to {output_path} ")
        parent = str(output_path.parent)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        logger.info(f'checking contents of {output_path.parent}')
        for subf in output_path.parent.iterdir():
            logger.info(subf)
            if subf.is_file() and subf.name.endswith(SUFFIX):
                existing[parent].add(subf)
        should_exist[parent].add(output_path)
        with open(output_path, "w") as f:
            f.write(
                generate_model_text(
                    key,
                    "sql",
                    value,
                    dialect=dialect,
                    dependencies=dependency_map.get(key, []),
                )
            )
        with open(output_path.parent / '__init__.py', "w") as f:
            pass
    if clear_target_dir:
        logger.info('clearing target directory')
        for key, paths in existing.items():
            logger.info(key)
            for path in paths:
                logger.info(path)
                if path not in should_exist[key]:
                    logger.info("Removing old file: %s", path)
                    os.remove(path)
    return output
