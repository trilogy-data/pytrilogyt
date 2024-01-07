from jinja2 import Template
from pathlib import Path
from preql import Executor, Environment  # noqa
from preql.dialect.enums import Dialects  # noqa
from datetime import datetime  # noqa
from pathlib import Path as PathlibPath  # noqa
from preql.hooks.query_debugger import DebuggingHook  # noqa
from preql.dialect.enums import Dialects  # noqa
from preqlt.constants import logger
from preql.core.models import ProcessedQueryPersist, ProcessedQuery

def generate_model_text(model_name, model_type, model_sql):
    template = Template(
        """
    {{ model_type }} "{{ model_name }}" {
        {{ model_sql }}
    }
    """
    )
    return template.render(model_name=model_name, model_type=model_type, model_sql=model_sql)


def generate_model(root:Path, preql_path:Path, base_namespace:str, dialect:Dialects):

    logger.info(f'Parsing file {preql_path} with dialect {dialect} and base namespace {base_namespace}')
    exec = Executor(
        dialect=dialect,
        engine=dialect.default_engine(),
        environment=Environment(working_path=preql_path.parent, namespace=base_namespace),
        # hooks=[DebuggingHook()] if debug else [],
    )
    outputs = {}
    with open(preql_path, "r") as f:
        script = f.read()
    queries = exec.parse_text(script)
    start = datetime.now()
    for idx, query in enumerate(queries):
        lstart = datetime.now()
        if isinstance(query, ProcessedQueryPersist):
            base = ProcessedQuery(
                output_columns=query.output_columns,
                ctes = query.ctes,
                base=query.base,
                joins=query.joins,
                grain=query.grain,
                limit=query.limit,
                where_clause=query.where_clause,
                order_by=query.order_by,
            )
            outputs[query.output_to.address.location.split('.')[-1]] =  exec.generator.compile_statement(base)
    
    for key, value in outputs.items():
        output_path = root / f'{key}_preqlt_gen_model.sql'
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w") as f:
            f.write(f"-- Generated from preql source: {preql_path}; do not edit manually")
            f.write("{{ config(materialized='table') }}\n")
            f.write(value)
    