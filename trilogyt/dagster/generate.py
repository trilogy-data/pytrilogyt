import os
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from jinja2 import Template
from trilogy import Environment, Executor
from trilogy.authoring import (
    Datasource,
    PersistStatement,
)
from trilogy.core.models.execute import UnionCTE
from trilogy.core.statements.execute import ProcessedQueryPersist
from trilogy.dialect.enums import Dialects
from trilogy.constants import Rendering
from trilogy.core.models.build import BuildDatasource
from trilogyt.constants import logger
from trilogyt.core import enrich_environment
from trilogyt.dagster.config import DagsterConfig
from trilogyt.dagster.constants import ALL_JOB_NAME, ENTRYPOINT_FILE, SUFFIX

DEFAULT_DESCRIPTION: str = "No description provided"


@dataclass
class ModelInput:
    name: str
    file_path: Path
    import_path: Path

    @property
    def python_import(self) -> str:
        return ".".join(self.import_path.with_suffix("").parts)


def generate_model_text(
    model_name: str,
    model_type: str,
    model_sql: str,
    dialect: Dialects,
    dependencies: list[str],
    config: DagsterConfig,
) -> str:
    if not dialect == Dialects.DUCK_DB:
        raise NotImplementedError(f"Unsupported dialect {dialect}")
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
           ''' {{model_sql}} '''
        )
    """
    )

    built_dependencies = [
        ModelInput(
            name=dep,
            file_path=Path(f"{dep}.py"),
            import_path=Path(f"{config.opt_import_root}.{dep}_gen_model.py"),
        )
        for dep in sorted(list(set(dependencies)))
    ]

    return template.render(
        model_name=model_name,
        model_type=model_type,
        model_sql=model_sql,
        dialect=dialect,
        deps=built_dependencies,
    )


def generate_entry_file(
    models: list[ModelInput],
    dialect: Dialects,  # config: DagsterConfig
    dagster_path: Path,
    config: DagsterConfig,
):
    extra_kwargs: dict[str, Any] = {}
    if dialect != Dialects.DUCK_DB:
        raise NotImplementedError(f"Unsupported dialect {dialect}")

    if dialect == Dialects.DUCK_DB:
        extra_kwargs["database"] = "dagster.db"
        extra_kwargs["connection_config"] = {"enable_external_access": False}
    template = Template(
        """
from pathlib import Path

import dagster as dg
from dagster_duckdb import DuckDBResource


@dg.definitions
def defs():
    return dg.Definitions.merge(
        dg.Definitions(
            resources={
                "{{dialect.value}}": DuckDBResource(
                    database="{{extra_kwargs["database"]}}", connection_config={"enable_external_access": False}
                )
            }
        ),
        dg.load_from_defs_folder(
            project_root=Path(__file__).parent.parent.parent,
        ),
    )
"""
    )
    contents = template.render(
        models=models,
        dialect=dialect,
        all_job_name=ALL_JOB_NAME,
        extra_kwargs=extra_kwargs,
    )
    # write the module file
    with open(dagster_path / 'src' / '__init__.py', "w") as f:
        f.write('')
    with open(dagster_path / config.dagster_asset_path / '__init__.py', "w") as f:
        f.write('')
    # write the config file
    with open(dagster_path / config.dagster_asset_path / ENTRYPOINT_FILE, "w") as f:
        f.write(contents)

    with open(dagster_path / "pyproject.toml", "w") as f:
        f.write('''[project]
name = "trilogy_dagster"
requires-python = ">=3.9,<=3.13.3"
version = "0.1.0"
dependencies = [
    "dagster==1.11.2",
]

[dependency-groups]
dev = [
    "dagster-webserver",
    "dagster-dg-cli",
]

[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[tool.dg]
directory_type = "project"

[tool.dg.project]
root_module = "src"

''')


def generate_dependency_map(
    pqueries,
    possible_dependencies: dict[str, Datasource],
    model_ds_mapping: dict[str, str],
    config: DagsterConfig,
    executor: Executor,
):
    dependency_map = defaultdict(list)
    output: list[ModelInput] = []
    output_map: dict[str, str] = {}
    output_data: dict[str, Datasource] = {}
    for _, query in enumerate(pqueries):
        depends_on: list[ModelInput] = []
        if isinstance(query, ProcessedQueryPersist):
            logger.info(f"Starting on {_}")
            target = query.output_to.address.location
            eligible = {k: v for k, v in possible_dependencies.items() if k != target}
            for cte in query.ctes:
                if isinstance(cte, UnionCTE):
                    continue
                # handle inlined datasources
                logger.info(f"checking cte {cte.name} with {eligible}")
                if cte.base_name_override in eligible:
                    if any(x.name == cte.base_name_override for x in depends_on):
                        continue
                    matched = model_ds_mapping.get(cte.base_name_override)
                    if matched:
                        depends_on.append(matched)
                for source in cte.source.datasources:
                    logger.info(source.identifier)
                    if not isinstance(source, BuildDatasource):
                        continue

                    matched = model_ds_mapping.get(source.identifier)
                    if matched:
                        depends_on.append(matched)
            # get our names to label the model
            key = query.output_to.address.location.split(".")[-1]
            output_map[key] = executor.generator.compile_statement(query)
            output_data[key] = query.datasource
            dependency_map[key] = depends_on
            output.append(
                ModelInput(
                    name=key,
                    file_path=config.get_asset_path(key),
                    import_path=config.get_asset_import_path(key),
                )
            )
            logger.info(depends_on)
    return dependency_map, output, output_map


def generate_name_ds_mapping(
    file: Path, content: str, dialect: Dialects
) -> dict[str, str]:
    env: Environment = Environment(
        working_path=file.parent if file else os.getcwd(),
        # namespace=config.namespace,
    )
    executor = Executor(
        dialect=dialect,
        engine=dialect.default_engine(),
        environment=env,
        rendering=Rendering(parameters=False),
    )
    executor.environment = enrich_environment(executor.environment)

    try:
        _, statements = executor.environment.parse(content)
    except Exception as e:
        raise SyntaxError(f"Unable to parse {content}" + str(e))
    output = {}
    for query in statements:
        if isinstance(query, PersistStatement):
            logger.info(f"Starting on {_}")
            key = query.datasource.address.location.split(".")[-1]
            output[query.datasource.identifier] = key
    return output


def generate_model(
    preql_body: str,
    preql_path: Path | None,
    dialect: Dialects,
    config: DagsterConfig,
    environment: Environment | None = None,
    clear_target_dir: bool = True,
    model_ds_mapping: dict[str, str] | None = None,
) -> list[ModelInput]:
    env: Environment = environment or Environment(
        working_path=preql_path.parent if preql_path else os.getcwd(),
        # namespace=config.namespace,
    )
    executor = Executor(
        dialect=dialect,
        engine=dialect.default_engine(),
        environment=env,
        rendering=Rendering(parameters=False),
    )
    executor.environment = enrich_environment(executor.environment)

    try:
        _, statements = executor.environment.parse(preql_body)
    except Exception as e:
        raise SyntaxError(f"Unable to parse {preql_body}" + str(e))

    possible_dependencies: dict[str, Datasource] = {
        d.identifier: d
        for d in executor.environment.datasources.values()
        if "." not in d.identifier
    }
    parsed = [z for z in statements if isinstance(z, PersistStatement)]

    logger.info("generating queries")
    logger.info(f"possible dependencies are {list(possible_dependencies.keys())}")

    pqueries = executor.generator.generate_queries(executor.environment, parsed)
    logger.info(f"got {len(pqueries)} queries: {Counter([type(c) for c in pqueries])}")

    logger.info("Writing queries to output files")
    dependency_map, output, output_map = generate_dependency_map(
        pqueries, possible_dependencies, model_ds_mapping, config, executor
    )
    logger.info(f"dependency_map: {dependency_map}")

    existing = defaultdict(set)
    should_exist = defaultdict(set)
    for key, value in output_map.items():
        output_path = config.get_asset_path(key)
        logger.info(f"writing {key} to {output_path} ")
        parent = str(output_path.parent)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        logger.info(f"checking contents of {output_path.parent}")
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
                    config=config,
                )
            )
        with open(output_path.parent / "__init__.py", "w") as f:
            pass
    if clear_target_dir:
        logger.info("clearing target directory")
        for key, paths in existing.items():
            logger.info(key)
            for path in paths:
                logger.info(path)
                if path not in should_exist[key]:
                    logger.info("Removing old file: %s", path)
                    os.remove(path)
    return output
